package router

import (
	"context"
	"google.golang.org/protobuf/proto"
	"maps"
	"sync"
	"sync/atomic"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	routing_dns "github.com/xtls/xray-core/features/routing/dns"
)

// Router is an implementation of routing.Router.
type Router struct {
	domainStrategy Config_DomainStrategy
	rules          atomic.Pointer[ruleSet]
	scriptPath     string
	script         *scriptEngine
	balancers      atomic.Pointer[map[string]*Balancer]
	dns            dns.Client

	ctx        context.Context
	ohm        outbound.Manager
	dispatcher routing.Dispatcher
	mu         sync.Mutex
}

// Route is an implementation of routing.Route.
type Route struct {
	routing.Context
	outboundGroupTags []string
	outboundTag       string
	ruleTag           string
	// Set only when this Route describes a *rule* (see ListRule) rather than a
	// routing decision for a live connection.
	userEmail  []string
	inboundTag []string
}

// Init initializes the Router.
func (r *Router) Init(ctx context.Context, config *Config, d dns.Client, ohm outbound.Manager, dispatcher routing.Dispatcher) error {
	r.domainStrategy = config.DomainStrategy
	r.scriptPath = config.Script
	r.dns = d
	r.ctx = ctx
	r.ohm = ohm
	r.dispatcher = dispatcher

	r.rules.Store(newRuleSet(nil))
	r.balancers.Store(&map[string]*Balancer{})
	return r.ReloadRules(config, false)
}

// PickRoute implements routing.Router.
func (r *Router) PickRoute(ctx routing.Context) (routing.Route, error) {
	if r.script != nil {
		return r.script.pickRoute(ctx)
	}

	originalCtx := ctx
	rule, ctx, err := r.pickRouteInternal(ctx)
	if err != nil {
		return nil, err
	}
	tag, err := rule.GetTag()
	if err != nil {
		return nil, err
	}
	if rule.Webhook != nil {
		rule.Webhook.Fire(originalCtx, tag)
	}
	return &Route{Context: ctx, outboundTag: tag, ruleTag: rule.RuleTag}, nil
}

// AddRule implements routing.Router.
func (r *Router) AddRule(config *serial.TypedMessage, shouldAppend bool) error {
	inst, err := config.GetInstance()
	if err != nil {
		return err
	}
	if c, ok := inst.(*Config); ok {
		return r.ReloadRules(c, shouldAppend)
	}
	return errors.New("AddRule: config type error")
}

// AddRules applies many configs under one lock and publishes one snapshot, so
// adding N rules costs O(N) instead of the O(N^2) of N AddRule calls (each of
// which copies the whole rule list). Configs apply in order; a config that fails
// is skipped as a whole and its error is returned at the same index, so callers
// retry only the failed slots. shouldAppend=false starts from an empty rule set
// and appends every config to it.
func (r *Router) AddRules(configs []*serial.TypedMessage, shouldAppend bool) []error {
	errs := make([]error, len(configs))
	r.mu.Lock()
	defer r.mu.Unlock()

	old := r.rules.Load()
	oldBalancers := *r.balancers.Load()
	var rules []*Rule
	balancers := make(map[string]*Balancer, len(oldBalancers))
	existTags := make(map[string]bool, len(old.all))
	if shouldAppend {
		rules = make([]*Rule, len(old.all), len(old.all)+len(configs))
		copy(rules, old.all)
		maps.Copy(balancers, oldBalancers)
		for _, rule := range old.all {
			existTags[rule.RuleTag] = true
		}
	}
	for i, msg := range configs {
		inst, err := msg.GetInstance()
		if err != nil {
			errs[i] = err
			continue
		}
		c, ok := inst.(*Config)
		if !ok {
			errs[i] = errors.New("AddRule: config type error")
			continue
		}
		added, addedBalancers, err := r.buildConfig(c, balancers, existTags)
		if err != nil {
			errs[i] = err
			continue
		}
		for _, rule := range added {
			existTags[rule.RuleTag] = true
		}
		rules = append(rules, added...)
		maps.Copy(balancers, addedBalancers)
	}
	r.balancers.Store(&balancers)
	r.rules.Store(newRuleSet(rules))
	if !shouldAppend {
		closeWebhooks(old.all)
	}
	return errs
}

func (r *Router) ReloadRules(config *Config, shouldAppend bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	old := r.rules.Load()
	oldBalancers := *r.balancers.Load()

	var newRules []*Rule
	newBalancers := make(map[string]*Balancer)
	existTags := make(map[string]bool, len(old.all)+len(config.Rule))
	if shouldAppend {
		newRules = make([]*Rule, len(old.all), len(old.all)+len(config.Rule))
		copy(newRules, old.all)
		maps.Copy(newBalancers, oldBalancers)
		for _, rule := range old.all {
			existTags[rule.RuleTag] = true
		}
	}

	added, addedBalancers, err := r.buildConfig(config, newBalancers, existTags)
	if err != nil {
		return err
	}
	maps.Copy(newBalancers, addedBalancers)
	newRules = append(newRules, added...)

	r.balancers.Store(&newBalancers)
	r.rules.Store(newRuleSet(newRules))
	if !shouldAppend {
		closeWebhooks(old.all)
	}
	return nil
}

// buildConfig turns one Config into rules and balancers without mutating the
// given balancer / tag state, so a failing config leaves nothing behind.
func (r *Router) buildConfig(config *Config, balancers map[string]*Balancer, existTags map[string]bool) ([]*Rule, map[string]*Balancer, error) {
	added := make(map[string]*Balancer)
	for _, rule := range config.BalancingRule {
		if _, found := balancers[rule.Tag]; found {
			return nil, nil, errors.New("duplicate balancer tag")
		}
		if _, found := added[rule.Tag]; found {
			return nil, nil, errors.New("duplicate balancer tag")
		}
		balancer, err := rule.Build(r.ohm, r.dispatcher)
		if err != nil {
			return nil, nil, err
		}
		balancer.InjectContext(r.ctx)
		added[rule.Tag] = balancer
	}

	rules := make([]*Rule, 0, len(config.Rule))
	seen := make(map[string]bool, len(config.Rule))
	fail := func(err error) ([]*Rule, map[string]*Balancer, error) {
		closeWebhooks(rules)
		return nil, nil, err
	}
	for _, rule := range config.Rule {
		cond, err := rule.BuildCondition()
		if err != nil {
			return fail(err)
		}
		rr := &Rule{
			Condition:    cond,
			Tag:          rule.GetTag(),
			RuleTag:      rule.GetRuleTag(),
			UserEmail:    rule.GetUserEmail(),
			InboundTag:   rule.GetInboundTag(),
			userKeys:     pureUserKeys(rule),
			nativeConfig: proto.Clone(rule).(*RoutingRule),
		}
		if rr.RuleTag != "" && (existTags[rr.RuleTag] || seen[rr.RuleTag]) {
			return fail(errors.New("duplicate ruleTag ", rr.RuleTag))
		}
		seen[rr.RuleTag] = true
		if wh := rule.GetWebhook(); wh != nil {
			notifier, err := NewWebhookNotifier(wh)
			if err != nil {
				return fail(err)
			}
			rr.Webhook = notifier
		}
		if btag := rule.GetBalancingTag(); len(btag) > 0 {
			brule, found := added[btag]
			if !found {
				brule, found = balancers[btag]
			}
			if !found {
				closeWebhooks([]*Rule{rr})
				return fail(errors.New("balancer ", btag, " not found"))
			}
			rr.Balancer = brule
		}
		rules = append(rules, rr)
	}
	return rules, added, nil
}

// RemoveRule implements routing.Router.
func (r *Router) RemoveRule(tag string) error {
	if tag == "" {
		return errors.New("empty tag name!")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	oldRules := r.rules.Load().all
	newRules := make([]*Rule, 0, len(oldRules))
	var removed []*Rule
	for _, rule := range oldRules {
		if rule.RuleTag != tag {
			newRules = append(newRules, rule)
		} else {
			removed = append(removed, rule)
		}
	}
	r.rules.Store(newRuleSet(newRules))
	closeWebhooks(removed)
	return nil
}

// ListRule implements routing.Router
func (r *Router) ListRule() []routing.Route {
	rules := r.rules.Load().all
	ruleList := make([]routing.Route, 0, len(rules))
	for _, rule := range rules {
		ruleList = append(ruleList, &Route{
			outboundTag: rule.Tag,
			ruleTag:     rule.RuleTag,
			userEmail:   rule.UserEmail,
			inboundTag:  rule.InboundTag,
		})
	}
	return ruleList
}

// ReplaceRule validates first, then publishes one snapshot without a gap in guards.
func (r *Router) ReplaceRule(message *serial.TypedMessage) error {
	value, err := message.GetInstance()
	if err != nil {
		return err
	}
	config, ok := value.(*Config)
	if !ok || len(config.Rule) != 1 || config.Rule[0].RuleTag == "" || len(config.BalancingRule) != 0 {
		return errors.New("replacement needs one named rule")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.rules.Load().all
	tag := config.Rule[0].RuleTag
	existing := make(map[string]bool, len(old))
	for _, rule := range old {
		if rule.RuleTag != tag {
			existing[rule.RuleTag] = true
		}
	}
	added, _, err := r.buildConfig(config, *r.balancers.Load(), existing)
	if err != nil {
		return err
	}
	next := make([]*Rule, 0, len(old)+1)
	replaced := false
	removed := []*Rule{}
	for _, rule := range old {
		if rule.RuleTag == tag {
			next = append(next, added[0])
			removed = append(removed, rule)
			replaced = true
		} else {
			next = append(next, rule)
		}
	}
	if !replaced {
		next = append(next, added[0])
	}
	r.rules.Store(newRuleSet(next))
	closeWebhooks(removed)
	return nil
}

func (r *Router) RuleConfigByTag() map[string][]byte {
	result := make(map[string][]byte)
	for _, rule := range r.rules.Load().all {
		if rule.nativeConfig == nil {
			continue
		}
		value, err := proto.MarshalOptions{Deterministic: true}.Marshal(rule.nativeConfig)
		if err == nil {
			result[rule.RuleTag] = value
		}
	}
	return result
}

func (r *Router) pickRouteInternal(ctx routing.Context) (*Rule, routing.Context, error) {
	// SkipDNSResolve is set from DNS module.
	// the DOH remote server maybe a domain name,
	// this prevents cycle resolving dead loop
	skipDNSResolve := ctx.GetSkipDNSResolve()

	if r.domainStrategy == Config_IpOnDemand && !skipDNSResolve {
		ctx = routing_dns.ContextWithDNSClient(ctx, r.dns)
	}

	rules := r.rules.Load()

	if rule := rules.pick(ctx); rule != nil {
		return rule, ctx, nil
	}

	if r.domainStrategy != Config_IpIfNonMatch || len(ctx.GetTargetDomain()) == 0 || skipDNSResolve {
		return nil, ctx, common.ErrNoClue
	}

	ctx = routing_dns.ContextWithDNSClient(ctx, r.dns)

	// Try applying rules again if we have IPs.
	if rule := rules.pick(ctx); rule != nil {
		return rule, ctx, nil
	}

	return nil, ctx, common.ErrNoClue
}

// Start implements common.Runnable.
func (r *Router) Start() error {
	if r.scriptPath != "" {
		engine, err := newScriptEngine(r.scriptPath, r)
		if err != nil {
			return errors.New("failed to initialize routing script").Base(err)
		}
		r.script = engine
	}
	return nil
}

// closeWebhooks closes all webhook notifiers in the given rule set.
func closeWebhooks(rules []*Rule) {
	for _, rule := range rules {
		if rule.Webhook != nil {
			rule.Webhook.Close()
		}
	}
}

// Close implements common.Closable.
func (r *Router) Close() error {
	if r.script != nil {
		r.script.close()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	closeWebhooks(r.rules.Load().all)
	return nil
}

// Type implements common.HasType.
func (*Router) Type() interface{} {
	return routing.RouterType()
}

// GetOutboundGroupTags implements routing.Route.
func (r *Route) GetOutboundGroupTags() []string {
	return r.outboundGroupTags
}

// GetOutboundTag implements routing.Route.
func (r *Route) GetOutboundTag() string {
	return r.outboundTag
}

func (r *Route) GetRuleTag() string {
	return r.ruleTag
}

// GetUser and GetInboundTag are promoted from the embedded routing.Context,
// which is nil for the Routes ListRule builds; answer from the rule instead of
// panicking on the nil interface.
func (r *Route) GetUser() string {
	if r.Context != nil {
		return r.Context.GetUser()
	}
	if len(r.userEmail) == 0 {
		return ""
	}
	return r.userEmail[0]
}

func (r *Route) GetInboundTag() string {
	if r.Context != nil {
		return r.Context.GetInboundTag()
	}
	if len(r.inboundTag) == 0 {
		return ""
	}
	return r.inboundTag[0]
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		r := new(Router)
		if err := core.RequireFeatures(ctx, func(d dns.Client, ohm outbound.Manager, dispatcher routing.Dispatcher) error {
			return r.Init(ctx, config.(*Config), d, ohm, dispatcher)
		}); err != nil {
			return nil, err
		}
		return r, nil
	}))
}
