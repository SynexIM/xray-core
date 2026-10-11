package router

import (
	"maps"

	"github.com/xtls/xray-core/app/capability"
	"github.com/xtls/xray-core/common/errors"
)

func init() { capability.Register("balancer.hotReload") }

// PutBalancers adds balancers or replaces the ones with the same tag. Rules hold
// a pointer to their balancer, so every rule pointing at a replaced balancer is
// copied with the new pointer and the whole change is published as one rule
// snapshot: no connection ever sees a rule without its balancer. A config that
// fails to build leaves the router unchanged.
func (r *Router) PutBalancers(configs []*BalancingRule) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	old := *r.balancers.Load()
	next := maps.Clone(old)
	replaced := make(map[*Balancer]*Balancer)
	for _, c := range configs {
		if c.GetTag() == "" {
			return errors.New("balancer tag is empty")
		}
		b, err := c.Build(r.ohm, r.dispatcher)
		if err != nil {
			return errors.New("balancer ", c.GetTag()).Base(err)
		}
		b.InjectContext(r.ctx)
		if prev := next[c.GetTag()]; prev != nil {
			replaced[prev] = b
		}
		next[c.GetTag()] = b
	}
	r.rebind(replaced)
	r.balancers.Store(&next)
	return nil
}

// RemoveBalancers deletes balancers by tag. Missing tags are fine (idempotent);
// a balancer still referenced by a rule is not removed and the call fails, so a
// rule never routes to a balancer that is gone.
func (r *Router) RemoveBalancers(tags []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	next := maps.Clone(*r.balancers.Load())
	for _, tag := range tags {
		b := next[tag]
		if b == nil {
			continue
		}
		for _, rule := range r.rules.Load().all {
			if rule.Balancer == b {
				return errors.New("balancer ", tag, " is still used by rule ", rule.RuleTag)
			}
		}
		delete(next, tag)
	}
	r.balancers.Store(&next)
	return nil
}

// rebind publishes a rule snapshot in which rules pointing at a replaced
// balancer point at its replacement. Caller holds r.mu.
func (r *Router) rebind(replaced map[*Balancer]*Balancer) {
	if len(replaced) == 0 {
		return
	}
	old := r.rules.Load().all
	rules := make([]*Rule, len(old))
	for i, rule := range old {
		if b := replaced[rule.Balancer]; b != nil {
			copied := *rule
			copied.Balancer = b
			rule = &copied
		}
		rules[i] = rule
	}
	r.rules.Store(newRuleSet(rules))
}
