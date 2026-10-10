package policy

import (
	"context"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/features/policy"
)

// Instance is an instance of Policy manager.
type Instance struct {
	levels atomic.Pointer[levelSnapshot]
	system *SystemPolicy
}

type levelSnapshot struct {
	levels map[uint32]*Policy
}

// SetLevels replaces the entire level table. Published policies are immutable.
func (m *Instance) SetLevels(levels map[uint32]*Policy) {
	snapshot := &levelSnapshot{levels: make(map[uint32]*Policy, len(levels))}
	for level, p := range levels {
		pp := defaultPolicy()
		if p != nil {
			pp.overrideWith(proto.Clone(p).(*Policy))
		}
		snapshot.levels[level] = pp
	}
	m.levels.Store(snapshot)
}

// GetLevels returns a detached copy of the effective level table.
func (m *Instance) GetLevels() map[uint32]*Policy {
	snapshot := m.levels.Load()
	levels := make(map[uint32]*Policy, len(snapshot.levels))
	for level, p := range snapshot.levels {
		levels[level] = proto.Clone(p).(*Policy)
	}
	return levels
}

// New creates new Policy manager instance.
func New(ctx context.Context, config *Config) (*Instance, error) {
	m := &Instance{
		system: config.System,
	}
	m.SetLevels(config.Level)

	return m, nil
}

// Type implements common.HasType.
func (*Instance) Type() interface{} {
	return policy.ManagerType()
}

// ForLevel implements policy.Manager.
func (m *Instance) ForLevel(level uint32) policy.Session {
	if p, ok := m.levels.Load().levels[level]; ok {
		return p.ToCorePolicy()
	}
	return policy.SessionDefault()
}

// ForSystem implements policy.Manager.
func (m *Instance) ForSystem() policy.System {
	if m.system == nil {
		return policy.System{}
	}
	return m.system.ToCorePolicy()
}

// Start implements common.Runnable.Start().
func (m *Instance) Start() error {
	return nil
}

// Close implements common.Closable.Close().
func (m *Instance) Close() error {
	return nil
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return New(ctx, config.(*Config))
	}))
}
