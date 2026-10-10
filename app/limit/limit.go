package limit

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/session"
	featurelimit "github.com/xtls/xray-core/features/limit"
	"google.golang.org/protobuf/proto"
)

const sourceIPIdle = 20 * time.Second

type AdmissionError struct{ RuleTag, Resource string }

func (e *AdmissionError) Error() string {
	return fmt.Sprintf("limit rule %q: %s 超限", e.RuleTag, e.Resource)
}

type ProfileInUseError struct {
	Name  string
	Rules []string
}

func (e *ProfileInUseError) Error() string {
	return fmt.Sprintf("profile %q 被规则引用: %s", e.Name, strings.Join(e.Rules, ", "))
}

type profileState struct {
	current atomic.Pointer[Profile]
	members map[*bucket]struct{} // Runtime.mu 保护，包括仍有旧连接的退役规则桶
}
type sourceState struct{ last time.Time }
type bucket struct {
	mu          sync.Mutex
	tag         string
	profile     *profileState
	up, down    *Shaper
	connections uint32
	ips         map[string]*sourceState
	idleSince   time.Time
	retired     bool        // Runtime.mu 保护
	fair        bucketUsage // 只由维护 goroutine 访问
}
type indexedRule struct {
	config                  *Rule
	user, inbound, outbound map[string]struct{}
	bucket                  *bucket
}
type ruleIndex map[string]map[string]*indexedRule

// Runtime 管理规则生命周期；令牌等待只锁每个方向的桶，不持有规则表锁。
type Runtime struct {
	mu                                         sync.RWMutex
	profiles                                   map[string]*profileState
	rules                                      map[string]*indexedRule
	users, inbounds, outbounds                 ruleIndex
	buckets                                    map[*bucket]struct{}
	node                                       *NodeLimit
	upBytes, downBytes                         atomic.Uint64
	participatingUp, participatingDown         atomic.Uint64
	tickMu                                     sync.Mutex
	nodeMu                                     sync.RWMutex
	nodeStatus                                 NodeStatus
	congestion                                 [2]congestionState
	lastUp, lastDown                           uint64
	lastParticipatingUp, lastParticipatingDown uint64
	lastTick                                   time.Time
	cancel                                     context.CancelFunc
	done                                       chan struct{}
	started, closed                            bool
}

func New(c *Config) (*Runtime, error) {
	if err := ValidateConfig(c); err != nil {
		return nil, err
	}
	r := &Runtime{
		profiles: map[string]*profileState{}, rules: map[string]*indexedRule{},
		users: ruleIndex{}, inbounds: ruleIndex{}, outbounds: ruleIndex{},
		buckets: map[*bucket]struct{}{}, done: make(chan struct{}), lastTick: time.Now(),
	}
	if err := r.PutProfiles(c.Profiles); err != nil {
		return nil, err
	}
	if err := r.AddRules(c.Rules); err != nil {
		return nil, err
	}
	if err := r.SetNode(c.Node); err != nil {
		return nil, err
	}
	return r, nil
}
func (*Runtime) Type() interface{} { return featurelimit.Type() }
func (r *Runtime) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errShaperClosed
	}
	if r.started {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.started = true
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				r.tick(now)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}
func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	started := r.started
	if r.cancel != nil {
		r.cancel()
	}
	buckets := make([]*bucket, 0, len(r.buckets))
	for b := range r.buckets {
		buckets = append(buckets, b)
	}
	r.mu.Unlock()
	if started {
		<-r.done
	}
	for _, b := range buckets {
		b.up.close()
		b.down.close()
	}
	return nil
}
func (r *Runtime) SetNode(n *NodeLimit) error {
	if err := ValidateNode(n); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errShaperClosed
	}
	if n == nil {
		r.node = nil
	} else {
		r.node = proto.Clone(n).(*NodeLimit)
	}
	return nil
}
func (r *Runtime) PutProfiles(profiles []*Profile) error {
	names := map[string]bool{}
	for _, p := range profiles {
		if err := ValidateProfile(p); err != nil {
			return err
		}
		if names[p.Name] {
			return invalid("profiles."+p.Name, "名称重复")
		}
		names[p.Name] = true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errShaperClosed
	}
	for _, p := range profiles {
		config := proto.Clone(p).(*Profile)
		state := r.profiles[p.Name]
		if state == nil {
			state = &profileState{members: map[*bucket]struct{}{}}
			r.profiles[p.Name] = state
		}
		state.current.Store(config)
		for b := range state.members {
			// 换参数而非换整形器，让已建连接和已排队的片继续使用原桶。
			b.up.Configure(config.Up)
			b.down.Configure(config.Down)
		}
	}
	return nil
}
func (r *Runtime) RemoveProfiles(names []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errShaperClosed
	}
	for _, name := range names {
		var references []string
		state := r.profiles[name]
		if state != nil {
			for b := range state.members {
				if !b.retired {
					references = append(references, b.tag)
				}
			}
		}
		if len(references) > 0 {
			sort.Strings(references)
			return &ProfileInUseError{Name: name, Rules: references}
		}
	}
	for _, name := range names {
		delete(r.profiles, name)
	}
	return nil
}
func stringSet(list []string) map[string]struct{} {
	result := make(map[string]struct{}, len(list))
	for _, s := range list {
		result[s] = struct{}{}
	}
	return result
}
func indexRule(index ruleIndex, keys map[string]struct{}, rule *indexedRule, add bool) {
	for key := range keys {
		if add {
			if index[key] == nil {
				index[key] = map[string]*indexedRule{}
			}
			index[key][rule.config.RuleTag] = rule
		} else {
			delete(index[key], rule.config.RuleTag)
			if len(index[key]) == 0 {
				delete(index, key)
			}
		}
	}
}
func (r *Runtime) unindex(rule *indexedRule) {
	indexRule(r.users, rule.user, rule, false)
	indexRule(r.inbounds, rule.inbound, rule, false)
	indexRule(r.outbounds, rule.outbound, rule, false)
	rule.bucket.retired = true
}
func (r *Runtime) AddRules(rules []*Rule) error {
	for _, rule := range rules {
		if err := ValidateRule(rule); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errShaperClosed
	}
	for _, rule := range rules {
		if r.profiles[rule.Profile] == nil {
			return invalid("rules."+rule.RuleTag+".profile", "引用的 profile 不存在: "+rule.Profile)
		}
	}
	for _, rule := range rules {
		config := proto.Clone(rule).(*Rule)
		if old := r.rules[rule.RuleTag]; old != nil {
			r.unindex(old)
		}
		state := r.profiles[rule.Profile]
		profile := state.current.Load()
		b := &bucket{tag: rule.RuleTag, profile: state, up: newShaper(), down: newShaper(), ips: map[string]*sourceState{}, idleSince: time.Now()}
		b.up.Configure(profile.Up)
		b.down.Configure(profile.Down)
		state.members[b] = struct{}{}
		r.buckets[b] = struct{}{}
		entry := &indexedRule{config: config, user: stringSet(rule.User), inbound: stringSet(rule.InboundTag), outbound: stringSet(rule.OutboundTag), bucket: b}
		r.rules[rule.RuleTag] = entry
		indexRule(r.users, entry.user, entry, true)
		indexRule(r.inbounds, entry.inbound, entry, true)
		indexRule(r.outbounds, entry.outbound, entry, true)
	}
	return nil
}
func (r *Runtime) RemoveRules(tags []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errShaperClosed
	}
	for _, tag := range tags {
		if old := r.rules[tag]; old != nil {
			r.unindex(old)
			delete(r.rules, tag)
		}
	}
	return nil
}

type RuleSummary struct{ RuleTag, Profile string }

func (r *Runtime) ListRules(offset, limit uint32) ([]RuleSummary, uint32) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tags := make([]string, 0, len(r.rules))
	for tag := range r.rules {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	start := min(uint64(offset), uint64(len(tags)))
	end := uint64(len(tags))
	if limit > 0 {
		end = min(end, start+uint64(limit))
	}
	result := make([]RuleSummary, 0, end-start)
	for _, tag := range tags[start:end] {
		result = append(result, RuleSummary{tag, r.rules[tag].config.Profile})
	}
	return result, uint32(len(tags))
}
func conditionMatches(set map[string]struct{}, value string) bool {
	if len(set) == 0 {
		return true
	}
	if value == "" {
		return false
	}
	_, ok := set[value]
	return ok
}
func (r *indexedRule) matches(info featurelimit.SessionInfo) bool {
	return conditionMatches(r.user, info.Email) && conditionMatches(r.inbound, info.InboundTag) && conditionMatches(r.outbound, info.OutboundTag)
}
func (b *bucket) expireIPs(now time.Time) {
	for ip, state := range b.ips {
		if now.Sub(state.last) >= sourceIPIdle {
			delete(b.ips, ip)
		}
	}
}
func (b *bucket) checkAdmission(ip string, now time.Time) error {
	b.expireIPs(now)
	p := b.profile.current.Load()
	if p.MaxConnections > 0 && b.connections >= p.MaxConnections {
		return &AdmissionError{b.tag, "connections"}
	}
	if ip != "" {
		_, exists := b.ips[ip]
		if !exists && p.MaxSourceIps > 0 && uint32(len(b.ips)) >= p.MaxSourceIps {
			return &AdmissionError{b.tag, "source IPs"}
		}
	}
	return nil
}
func (b *bucket) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.connections--
	if b.connections == 0 {
		b.idleSince = time.Now()
	}
}
func (b *bucket) touch(ip string) {
	if ip == "" {
		return
	}
	b.mu.Lock()
	if state := b.ips[ip]; state != nil {
		state.last = time.Now()
	} else {
		b.ips[ip] = &sourceState{last: time.Now()}
	}
	b.mu.Unlock()
}
func (r *Runtime) Attach(ctx context.Context, info featurelimit.SessionInfo) (featurelimit.WrapReader, featurelimit.WrapWriter, func(), error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, nil, nil, errShaperClosed
	}
	candidates := map[string]*indexedRule{}
	for _, index := range []map[string]*indexedRule{r.users[info.Email], r.inbounds[info.InboundTag], r.outbounds[info.OutboundTag]} {
		for tag, rule := range index {
			candidates[tag] = rule
		}
	}
	matches := make([]*bucket, 0, len(candidates))
	for _, rule := range candidates {
		if rule.matches(info) {
			matches = append(matches, rule.bucket)
		}
	}
	// 稳定顺序也让多桶准入不会因 map 顺序产生不同结果。
	sort.Slice(matches, func(i, j int) bool { return matches[i].tag < matches[j].tag })
	now := time.Now()
	for _, b := range matches {
		b.mu.Lock()
	}
	for _, b := range matches {
		if err := b.checkAdmission(info.SourceIP, now); err != nil {
			for _, locked := range matches {
				locked.mu.Unlock()
			}
			r.mu.RUnlock()
			return nil, nil, nil, err
		}
	}
	for _, b := range matches {
		if info.SourceIP != "" {
			b.ips[info.SourceIP] = &sourceState{last: now}
		}
		b.connections++
		b.idleSince = time.Time{}
		b.mu.Unlock()
	}
	r.mu.RUnlock()
	if session.InboundFromContext(ctx) != nil {
		// 节点利用率包含不命中规则的流量；直通会绕过计量，热开 NodeLimit 也无法看到旧连接。
		// 未命中规则只计量，不创建速率桶、不排队；未加载模块仍保留上游直通。
		session.DisableSplice(ctx)
	}
	up := make([]*Flow, 0, len(matches))
	down := make([]*Flow, 0, len(matches))
	for _, b := range matches {
		up = append(up, b.up.NewFlow())
		down = append(down, b.down.NewFlow())
	}
	activity := func() bool {
		participates := false
		for _, b := range matches {
			b.touch(info.SourceIP)
			participates = participates || b.profile.current.Load().Congested != nil
		}
		return participates
	}
	upPacer := &connectionPacer{flows: up, activity: activity, meter: &r.upBytes, participatingMeter: &r.participatingUp, datagram: info.Datagram}
	downPacer := &connectionPacer{flows: down, activity: activity, meter: &r.downBytes, participatingMeter: &r.participatingDown, datagram: info.Datagram}
	var once sync.Once
	release := func() {
		once.Do(func() {
			for _, f := range up {
				f.release()
			}
			for _, f := range down {
				f.release()
			}
			for _, b := range matches {
				b.release()
			}
		})
	}
	return func(reader buf.Reader) buf.Reader { return newPacedReader(ctx, reader, upPacer) },
		func(writer buf.Writer) buf.Writer { return newPacedWriter(ctx, writer, downPacer) }, release, nil
}

type RuleStatus struct {
	RuleTag, Profile       string
	Up, Down               DirectionStatus
	Connections, SourceIPs uint32
}

func (r *Runtime) GetRuleStatus(tag string) (RuleStatus, bool) {
	r.mu.RLock()
	rule := r.rules[tag]
	r.mu.RUnlock()
	if rule == nil {
		return RuleStatus{}, false
	}
	b := rule.bucket
	b.mu.Lock()
	b.expireIPs(time.Now())
	result := RuleStatus{RuleTag: tag, Profile: rule.config.Profile, Connections: b.connections, SourceIPs: uint32(len(b.ips))}
	b.mu.Unlock()
	result.Up = b.up.Snapshot()
	result.Down = b.down.Snapshot()
	return result, true
}
func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(_ context.Context, c interface{}) (interface{}, error) { return New(c.(*Config)) }))
}

var _ featurelimit.Limiter = (*Runtime)(nil)
