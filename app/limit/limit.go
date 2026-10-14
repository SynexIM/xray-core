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
	active      bool
	retired     bool        // Runtime.mu 保护
	fair        bucketUsage // 只由维护 goroutine 访问
}
type indexedRule struct {
	config                  *Rule
	user, inbound, outbound map[string]struct{}
	bucket                  *bucket
}

// Runtime 管理规则生命周期；令牌等待只锁每个方向的桶，不持有规则表锁。
type Runtime struct {
	mu                                         sync.RWMutex
	profiles                                   map[string]*profileState
	rules                                      map[string]*indexedRule
	snapshot                                   atomic.Pointer[ruleSnapshot]
	active                                     [activeBucketShards]activeBucketShard
	retired                                    []*bucket
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
		buckets: map[*bucket]struct{}{}, done: make(chan struct{}), lastTick: time.Now(),
	}
	for index := range r.active {
		r.active[index].buckets = make(map[*bucket]struct{})
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
	r.snapshot.Store(&ruleSnapshot{closed: true})
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
func (r *Runtime) unindex(rule *indexedRule) {
	rule.bucket.retired = true
	r.retired = append(r.retired, rule.bucket)
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
	}
	r.publishRulesLocked()
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
	r.publishRulesLocked()
	return nil
}

type RuleSummary struct{ RuleTag, Profile string }

func (r *Runtime) ListRules(offset, limit uint32) ([]RuleSummary, uint32) {
	snapshot := r.snapshot.Load()
	start := min(uint64(offset), uint64(len(snapshot.summaries)))
	end := uint64(len(snapshot.summaries))
	if limit > 0 {
		end = min(end, start+uint64(limit))
	}
	return append([]RuleSummary(nil), snapshot.summaries[start:end]...), uint32(len(snapshot.summaries))
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
	var matches []*bucket
	for {
		snapshot := r.snapshot.Load()
		if snapshot.closed {
			return nil, nil, nil, errShaperClosed
		}
		matches = matchingBuckets(snapshot, info)
		for _, bucket := range matches {
			bucket.mu.Lock()
		}
		if snapshot != r.snapshot.Load() {
			for _, bucket := range matches {
				bucket.mu.Unlock()
			}
			continue
		}
		now := time.Now()
		for _, bucket := range matches {
			if err := bucket.checkAdmission(info.SourceIP, now); err != nil {
				for _, locked := range matches {
					locked.mu.Unlock()
				}
				return nil, nil, nil, err
			}
		}
		for _, bucket := range matches {
			if info.SourceIP != "" {
				bucket.ips[info.SourceIP] = &sourceState{last: now}
			}
			bucket.connections++
			bucket.idleSince = time.Time{}
			r.activateLocked(bucket)
			bucket.mu.Unlock()
		}
		break
	}

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
	upPacer := newConnectionPacer(up, activity, &r.upBytes, &r.participatingUp, info.Datagram)
	downPacer := newConnectionPacer(down, activity, &r.downBytes, &r.participatingDown, info.Datagram)
	var once sync.Once
	release := func() {
		once.Do(func() {
			upPacer.close()
			downPacer.close()
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
	if !info.Datagram && len(matches) > 0 {
		socket, err := newSocketPacer(ctx)
		if err != nil {
			release()
			return nil, nil, nil, err
		}
		downPacer.socket = socket
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
	snapshot := r.snapshot.Load()
	position := sort.Search(len(snapshot.ordered), func(index int) bool { return snapshot.ordered[index].config.RuleTag >= tag })
	if position == len(snapshot.ordered) || snapshot.ordered[position].config.RuleTag != tag {
		return RuleStatus{}, false
	}
	rule := snapshot.ordered[position]

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
