package stats

import "sync/atomic"

// Counter is an implementation of stats.Counter.
type Counter struct {
	value int64
	owner *userRecord
}

// Value implements stats.Counter.
func (c *Counter) Value() int64 {
	return atomic.LoadInt64(&c.value)
}

// Set implements stats.Counter.
func (c *Counter) Set(newValue int64) int64 {
	previous := atomic.SwapInt64(&c.value, newValue)
	if c.owner != nil && newValue != 0 && previous != newValue {
		c.owner.markChanged()
	}
	return previous
}

// Add implements stats.Counter.
func (c *Counter) Add(delta int64) int64 {
	value := atomic.AddInt64(&c.value, delta)
	if c.owner != nil && delta != 0 {
		c.owner.markChanged()
	}
	return value
}
