package limit

import (
	featurelimit "github.com/xtls/xray-core/features/limit"
	"slices"
)

type ruleSnapshot struct {
	closed                     bool
	users, inbounds, outbounds map[string][]*indexedRule
	ordered                    []*indexedRule
	summaries                  []RuleSummary
}

func (runtime *Runtime) publishRulesLocked() {
	snapshot := &ruleSnapshot{users: make(map[string][]*indexedRule), inbounds: make(map[string][]*indexedRule), outbounds: make(map[string][]*indexedRule), ordered: make([]*indexedRule, 0, len(runtime.rules))}
	for _, rule := range runtime.rules {
		snapshot.ordered = append(snapshot.ordered, rule)
	}
	slices.SortFunc(snapshot.ordered, func(first, second *indexedRule) int {
		if first.config.RuleTag < second.config.RuleTag {
			return -1
		}
		if first.config.RuleTag > second.config.RuleTag {
			return 1
		}
		return 0
	})
	for _, rule := range snapshot.ordered {
		snapshot.summaries = append(snapshot.summaries, RuleSummary{RuleTag: rule.config.RuleTag, Profile: rule.config.Profile})
		for key := range rule.user {
			snapshot.users[key] = append(snapshot.users[key], rule)
		}
		for key := range rule.inbound {
			snapshot.inbounds[key] = append(snapshot.inbounds[key], rule)
		}
		for key := range rule.outbound {
			snapshot.outbounds[key] = append(snapshot.outbounds[key], rule)
		}
	}
	runtime.snapshot.Store(snapshot)
}

func matchingBuckets(snapshot *ruleSnapshot, info featurelimit.SessionInfo) []*bucket {
	indexes := [3][]*indexedRule{snapshot.users[info.Email], snapshot.inbounds[info.InboundTag], snapshot.outbounds[info.OutboundTag]}
	var positions [3]int
	var matches []*bucket
	for {
		var next *indexedRule
		for index, entries := range indexes {
			if positions[index] < len(entries) {
				candidate := entries[positions[index]]
				if next == nil || candidate.config.RuleTag < next.config.RuleTag {
					next = candidate
				}
			}
		}
		if next == nil {
			return matches
		}
		for index, entries := range indexes {
			if positions[index] < len(entries) && entries[positions[index]] == next {
				positions[index]++
			}
		}
		if next.matches(info) {
			matches = append(matches, next.bucket)
		}
	}
}
