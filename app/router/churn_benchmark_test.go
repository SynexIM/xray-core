package router

import (
	"fmt"
	"testing"
)

func BenchmarkRemoveRouteBatch(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			all := make([]*Rule, n)
			for i := range all {
				all[i] = &Rule{RuleTag: fmt.Sprint(i), userKeys: []string{fmt.Sprint(i)}}
			}
			tags := make([]string, min(n, 1000))
			for i := range tags {
				tags[i] = fmt.Sprint(i)
			}
			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				b.StopTimer()
				r := &Router{}
				r.rules.Store(newRuleSet(all))
				b.StartTimer()
				if batch, ok := any(r).(interface{ RemoveRules([]string) []error }); ok {
					for _, err := range batch.RemoveRules(tags) {
						if err != nil {
							b.Fatal(err)
						}
					}
				} else {
					for _, tag := range tags {
						if err := r.RemoveRule(tag); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
		})
	}
}
