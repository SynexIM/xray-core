package router

import (
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/xtls/xray-core/features/routing"
)

// ruleSet is an immutable snapshot of the rule list plus a lookup index.
//
// Rules whose only condition is "user email is one of these literals" (the shape
// a per-client route takes) are removed from the linear scan and indexed by
// email. Everything else stays in `others`, in order. Rule order still decides:
// pick returns whichever of "first indexed rule for this user" and "first other
// rule that applies" comes earlier in `all`, which is exactly what a full linear
// scan would return.
type ruleSet struct {
	all    []*Rule
	others []int          // positions in all of rules that are not indexed
	byUser map[string]int // email -> position of the first indexed rule naming it
}

func newRuleSet(rules []*Rule) *ruleSet {
	s := &ruleSet{all: rules, byUser: make(map[string]int)}
	for i, rule := range rules {
		if len(rule.userKeys) == 0 {
			s.others = append(s.others, i)
			continue
		}
		for _, k := range rule.userKeys {
			if _, ok := s.byUser[k]; !ok {
				s.byUser[k] = i
			}
		}
	}
	return s
}

// pick returns the first rule in order that applies to ctx, or nil.
func (s *ruleSet) pick(ctx routing.Context) *Rule {
	limit := len(s.all)
	if user := ctx.GetUser(); user != "" {
		if i, ok := s.byUser[user]; ok {
			limit = i
		}
	}
	for _, i := range s.others {
		if i >= limit {
			break
		}
		if s.all[i].Apply(ctx) {
			return s.all[i]
		}
	}
	if limit < len(s.all) {
		return s.all[limit]
	}
	return nil
}

// ruleConditionFree lists the RoutingRule fields that are not matching
// conditions. Any other populated field makes the rule non-indexable, so a
// condition added to the proto later is safe by default.
var ruleConditionFree = map[string]bool{
	"user_email": true, "rule_tag": true, "webhook": true,
	"tag": true, "balancing_tag": true,
}

// pureUserKeys returns the literal emails of a rule that matches on user email
// alone, or nil if the rule has any other condition or any regexp entry.
// Empty entries are dropped (UserMatcher ignores them); a rule left with no keys
// can never match, so it is also correctly absent from the index and scan.
func pureUserKeys(rr *RoutingRule) []string {
	if len(rr.UserEmail) == 0 {
		return nil
	}
	pure := true
	rr.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if !ruleConditionFree[string(fd.Name())] {
			pure = false
			return false
		}
		return true
	})
	if !pure {
		return nil
	}
	keys := make([]string, 0, len(rr.UserEmail))
	for _, u := range rr.UserEmail {
		if len(u) > 7 && strings.HasPrefix(u, "regexp:") {
			return nil
		}
		if u != "" {
			keys = append(keys, u)
		}
	}
	if len(keys) == 0 {
		// Never matches; leaving it in the linear scan is harmless.
		return nil
	}
	return keys
}
