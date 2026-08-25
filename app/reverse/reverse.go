package reverse

import (
	"context"
	"sync"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	core "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
)

const (
	internalDomain = "reverse"
)

func isDomain(dest net.Destination, domain string) bool {
	return dest.Address.Family().IsDomain() && dest.Address.Domain() == domain
}

func isInternalDomain(dest net.Destination) bool {
	return isDomain(dest, internalDomain)
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		r := new(Reverse)
		if err := core.RequireFeatures(ctx, func(d routing.Dispatcher, om outbound.Manager) error {
			return r.Init(config.(*Config), d, om)
		}); err != nil {
			return nil, err
		}
		return r, nil
	}))
}

// Reverse 持有本节点上所有 bridge / portal。
//
// 相比上游多了一把锁、dispatcher/ohm 的引用和 started 标志：为的是让
// app/reverse/command 能在**进程不重启**的前提下加减 bridge/portal。
// API 调用方切换入口是常规操作，如果每次都要重启 xray，那台节点上所有
// 其他客户的连接会一起断——热改是产品要求，不是优化。
type Reverse struct {
	access  sync.Mutex
	d       routing.Dispatcher
	ohm     outbound.Manager
	started bool
	bridges []*Bridge
	portals []*Portal
}

func (r *Reverse) Init(config *Config, d routing.Dispatcher, ohm outbound.Manager) error {
	r.access.Lock()
	defer r.access.Unlock()

	// 存下来：热加时要用同一个 dispatcher / outbound manager。
	r.d = d
	r.ohm = ohm

	for _, bConfig := range config.BridgeConfig {
		b, err := NewBridge(bConfig, d)
		if err != nil {
			return err
		}
		r.bridges = append(r.bridges, b)
	}

	for _, pConfig := range config.PortalConfig {
		p, err := NewPortal(pConfig, ohm)
		if err != nil {
			return err
		}
		r.portals = append(r.portals, p)
	}

	return nil
}

func (r *Reverse) Type() interface{} {
	return (*Reverse)(nil)
}

func (r *Reverse) Start() error {
	r.access.Lock()
	defer r.access.Unlock()

	for _, b := range r.bridges {
		if err := b.Start(); err != nil {
			return err
		}
	}

	for _, p := range r.portals {
		if err := p.Start(); err != nil {
			return err
		}
	}

	// 之后热加进来的 bridge/portal 要立刻启动，不能等一个永远不会再来的 Start。
	r.started = true
	return nil
}

func (r *Reverse) Close() error {
	r.access.Lock()
	defer r.access.Unlock()

	r.started = false
	var errs []error
	for _, b := range r.bridges {
		errs = append(errs, b.Close())
	}

	for _, p := range r.portals {
		errs = append(errs, p.Close())
	}

	return errors.Combine(errs...)
}

// AddBridge 热加一个 bridge。tag 必须唯一——重复的 tag 会让路由规则指向哪一个
// 变成靠运气，这种错必须当场报出来，不能等到流量走错才发现。
func (r *Reverse) AddBridge(config *BridgeConfig) error {
	r.access.Lock()
	defer r.access.Unlock()

	if config == nil {
		return errors.New("nil bridge config")
	}
	for _, b := range r.bridges {
		if b.Tag() == config.Tag {
			return errors.New("bridge already exists: ", config.Tag)
		}
	}
	b, err := NewBridge(config, r.d)
	if err != nil {
		return err
	}
	if r.started {
		if err := b.Start(); err != nil {
			return err
		}
	}
	r.bridges = append(r.bridges, b)
	return nil
}

// RemoveBridge 停掉一个 bridge：它不再建新的 worker。
//
// 已经建起来的 worker 不强杀——一是它们各自带 60 秒无活动自动回收
// （见 NewBridgeWorker 的 ActivityTimer），二是强杀会和 monitor 那个周期任务
// 抢 b.workers。让存量连接自然收敛，正是热改想要的效果。
func (r *Reverse) RemoveBridge(tag string) error {
	r.access.Lock()
	defer r.access.Unlock()

	for i, b := range r.bridges {
		if b.Tag() != tag {
			continue
		}
		err := b.Close()
		r.bridges = append(r.bridges[:i], r.bridges[i+1:]...)
		return err
	}
	return errors.New("bridge not found: ", tag)
}

// AddPortal 热加一个 portal。portal 会往 outbound manager 注册一个同名 handler，
// tag 撞车时那边也会报错，这里先挡一道，错误信息更直接。
func (r *Reverse) AddPortal(config *PortalConfig) error {
	r.access.Lock()
	defer r.access.Unlock()

	if config == nil {
		return errors.New("nil portal config")
	}
	for _, p := range r.portals {
		if p.Tag() == config.Tag {
			return errors.New("portal already exists: ", config.Tag)
		}
	}
	p, err := NewPortal(config, r.ohm)
	if err != nil {
		return err
	}
	if r.started {
		if err := p.Start(); err != nil {
			return err
		}
	}
	r.portals = append(r.portals, p)
	return nil
}

// RemovePortal 摘掉一个 portal，同时摘掉它在 outbound manager 上的 handler。
func (r *Reverse) RemovePortal(tag string) error {
	r.access.Lock()
	defer r.access.Unlock()

	for i, p := range r.portals {
		if p.Tag() != tag {
			continue
		}
		var err error
		if r.started {
			// 没启动过就没注册过 handler，这时候 RemoveHandler 只会白报一个错。
			err = p.Close()
		}
		r.portals = append(r.portals[:i], r.portals[i+1:]...)
		return err
	}
	return errors.New("portal not found: ", tag)
}

// List 返回当前生效的配置快照。API 调用方靠它确认「我改的到底进去没有」，
// 所以返回的是运行中对象的实况，不是启动时那份配置的副本。
func (r *Reverse) List() ([]*BridgeConfig, []*PortalConfig) {
	r.access.Lock()
	defer r.access.Unlock()

	bridges := make([]*BridgeConfig, 0, len(r.bridges))
	for _, b := range r.bridges {
		bridges = append(bridges, &BridgeConfig{Tag: b.Tag(), Domain: b.Domain()})
	}
	portals := make([]*PortalConfig, 0, len(r.portals))
	for _, p := range r.portals {
		portals = append(portals, &PortalConfig{Tag: p.Tag(), Domain: p.Domain()})
	}
	return bridges, portals
}
