package router

import (
	"math"
	"strings"

	"github.com/google/btree"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/xtls/xray-core/features/routing"
)

// rulePosition orders rules: larger priority first, then insertion order.
// rank is the negated priority so that ascending order is matching order.
type rulePosition struct {
	rank int64
	seq  uint64
}

func (p rulePosition) less(q rulePosition) bool {
	if p.rank != q.rank {
		return p.rank < q.rank
	}
	return p.seq < q.seq
}

var (
	firstPosition = rulePosition{rank: math.MinInt64}
	afterLast     = rulePosition{rank: math.MaxInt64, seq: math.MaxUint64}
)

type orderedRule struct {
	position rulePosition
	rule     *Rule
}

type taggedRule struct {
	tag      string
	position rulePosition
	rule     *Rule
}

type userRuleEntry struct {
	email    string
	position rulePosition
	rule     *Rule
}

// 复用 btree 的写时复制；单条写入只复制查找路径，发布时仍是一份完整快照。
type ruleSet struct {
	ordered      *btree.BTreeG[orderedRule]
	others       *btree.BTreeG[orderedRule]
	byTag        *btree.BTreeG[taggedRule]
	byUser       *btree.BTreeG[userRuleEntry]
	nextPosition uint64
}

func newRuleSet(rules []*Rule) *ruleSet {
	lessPosition := func(first, second orderedRule) bool { return first.position.less(second.position) }
	set := &ruleSet{
		ordered: btree.NewG(32, lessPosition),
		others:  btree.NewG(32, lessPosition),
		byTag: btree.NewG(32, func(first, second taggedRule) bool {
			return first.tag < second.tag
		}),
		byUser: btree.NewG(32, func(first, second userRuleEntry) bool {
			if first.email != second.email {
				return first.email < second.email
			}
			return first.position.less(second.position)
		}),
	}
	set.append(rules)
	return set
}

// Clone 只由 Router.mu 下的写者调用；已发布的树仅做查询和遍历。
func (set *ruleSet) clone() *ruleSet {
	return &ruleSet{
		ordered: set.ordered.Clone(), others: set.others.Clone(),
		byTag: set.byTag.Clone(), byUser: set.byUser.Clone(), nextPosition: set.nextPosition,
	}
}

func (set *ruleSet) append(rules []*Rule) {
	for _, rule := range rules {
		position := rulePosition{rank: -int64(rule.nativeConfig.GetPriority()), seq: set.nextPosition}
		set.nextPosition++
		entry := orderedRule{position: position, rule: rule}
		set.ordered.ReplaceOrInsert(entry)
		if rule.RuleTag != "" {
			set.byTag.ReplaceOrInsert(taggedRule{tag: rule.RuleTag, position: position, rule: rule})
		}
		if len(rule.userKeys) == 0 {
			set.others.ReplaceOrInsert(entry)
		}
		for _, email := range rule.userKeys {
			set.byUser.ReplaceOrInsert(userRuleEntry{email: email, position: position, rule: rule})
		}
	}
}

func (set *ruleSet) hasTag(tag string) bool {
	return set.byTag.Has(taggedRule{tag: tag})
}

func (set *ruleSet) remove(tag string) *Rule {
	entry, found := set.byTag.Delete(taggedRule{tag: tag})
	if !found {
		return nil
	}
	set.ordered.Delete(orderedRule{position: entry.position})
	set.others.Delete(orderedRule{position: entry.position})
	for _, email := range entry.rule.userKeys {
		set.byUser.Delete(userRuleEntry{email: email, position: entry.position})
	}
	return entry.rule
}

func (set *ruleSet) allRules() []*Rule {
	rules := make([]*Rule, 0, set.ordered.Len())
	set.ordered.Ascend(func(entry orderedRule) bool {
		rules = append(rules, entry.rule)
		return true
	})
	return rules
}

func (set *ruleSet) pick(ctx routing.Context) *Rule {
	limit := afterLast
	var picked *Rule
	if email := ctx.GetUser(); email != "" {
		set.byUser.AscendGreaterOrEqual(userRuleEntry{email: email, position: firstPosition}, func(entry userRuleEntry) bool {
			if entry.email == email {
				limit = entry.position
				picked = entry.rule
			}
			return false
		})
	}
	set.others.Ascend(func(entry orderedRule) bool {
		if !entry.position.less(limit) {
			return false
		}
		if entry.rule.Apply(ctx) {
			picked = entry.rule
			return false
		}
		return true
	})
	return picked
}

// ruleConditionFree lists the RoutingRule fields that are not matching
// conditions. Any other populated field makes the rule non-indexable, so a
// condition added to the proto later is safe by default.
var ruleConditionFree = map[string]bool{
	"user_email": true, "rule_tag": true, "webhook": true,
	"tag": true, "balancing_tag": true, "priority": true,
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
