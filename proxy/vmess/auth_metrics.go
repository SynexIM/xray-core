package vmess

import (
	"sync/atomic"
	"time"
)

var authenticationBoundsMicros = [...]int64{1, 2, 4, 8, 16, 32, 64, 128, 200, 256, 512, 1000, 2000, 5000, 10000}

type authenticationHistogram struct {
	counts [len(authenticationBoundsMicros) + 1]atomic.Uint64
}
type AuthenticationHistogram struct {
	BoundsMicros []int64
	Counts       []uint64
}

func (histogram *authenticationHistogram) observe(elapsed time.Duration) {
	nanoseconds := elapsed.Nanoseconds()
	for index, bound := range authenticationBoundsMicros {
		if nanoseconds <= bound*1000 {
			histogram.counts[index].Add(1)
			return
		}
	}
	histogram.counts[len(authenticationBoundsMicros)].Add(1)
}
func (validator *TimedUserValidator) AuthenticationHistogram() AuthenticationHistogram {
	result := AuthenticationHistogram{BoundsMicros: append([]int64(nil), authenticationBoundsMicros[:]...), Counts: make([]uint64, len(validator.authentication.counts))}
	for index := range result.Counts {
		result.Counts[index] = validator.authentication.counts[index].Load()
	}
	return result
}
