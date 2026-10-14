//go:build !linux

package limit

import "context"

type socketPacer struct{}

func newSocketPacer(context.Context) (*socketPacer, error) { return nil, nil }
func (*socketPacer) update(float64) error                  { return nil }
