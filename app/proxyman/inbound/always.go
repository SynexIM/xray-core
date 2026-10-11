package inbound

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/mux"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport/internet"
	"google.golang.org/protobuf/proto"
)

func getStatCounter(v *core.Instance, tag string) (stats.Counter, stats.Counter) {
	var uplinkCounter stats.Counter
	var downlinkCounter stats.Counter

	policy := v.GetFeature(policy.ManagerType()).(policy.Manager)
	if len(tag) > 0 && policy.ForSystem().Stats.InboundUplink {
		statsManager := v.GetFeature(stats.ManagerType()).(stats.Manager)
		name := "inbound>>>" + tag + ">>>traffic>>>uplink"
		c, _ := statsManager.GetOrRegisterCounter(name)
		if c != nil {
			uplinkCounter = c
		}
	}
	if len(tag) > 0 && policy.ForSystem().Stats.InboundDownlink {
		statsManager := v.GetFeature(stats.ManagerType()).(stats.Manager)
		name := "inbound>>>" + tag + ">>>traffic>>>downlink"
		c, _ := statsManager.GetOrRegisterCounter(name)
		if c != nil {
			downlinkCounter = c
		}
	}

	return uplinkCounter, downlinkCounter
}

type AlwaysOnInboundHandler struct {
	lifecycle      sync.Mutex
	running        bool
	closed         bool
	proxyConfig    interface{}
	receiverConfig *proxyman.ReceiverConfig
	sniffing       *atomic.Pointer[session.SniffingRequest]
	proxy          proxy.Inbound
	workers        []worker
	mux            *mux.Server
	tag            string
}

func NewAlwaysOnInboundHandler(ctx context.Context, tag string, receiverConfig *proxyman.ReceiverConfig, proxyConfig interface{}) (*AlwaysOnInboundHandler, error) {
	sniffingRequest, err := proxyman.BuildSniffingRequest(receiverConfig.SniffingSettings)
	if err != nil {
		return nil, err
	}
	sniffing := &atomic.Pointer[session.SniffingRequest]{}
	sniffing.Store(&sniffingRequest)
	src := net.TCPDestination(net.AnyIP, 0)
	if receiverConfig.Listen != nil {
		src.Address = receiverConfig.Listen.AsAddress()
	}
	if receiverConfig.PortList != nil && len(receiverConfig.PortList.Range) > 0 {
		src.Port = net.Port(receiverConfig.PortList.Range[0].From)
	}
	mss, err := internet.ToMemoryStreamConfig(receiverConfig.StreamSettings)
	if err != nil {
		return nil, errors.New("failed to parse stream config").Base(err)
	}

	newCtx := session.ContextWithInbound(ctx, &session.Inbound{Tag: tag, Source: src})
	newCtx = session.ContextWithContent(newCtx, &session.Content{SniffingRequest: sniffingRequest})
	newCtx = session.ContextWithStreamSettings(newCtx, mss)

	rawProxy, err := common.CreateObject(newCtx, proxyConfig)
	if err != nil {
		return nil, err
	}
	p, ok := rawProxy.(proxy.Inbound)
	if !ok {
		return nil, errors.New("not an inbound proxy.")
	}

	h := &AlwaysOnInboundHandler{
		receiverConfig: receiverConfig,
		sniffing:       sniffing,
		proxyConfig:    proxyConfig,
		proxy:          p,
		mux:            mux.NewServer(ctx),
		tag:            tag,
	}

	uplinkCounter, downlinkCounter := getStatCounter(core.MustFromContext(ctx), tag)

	nl := p.Network()
	pl := receiverConfig.PortList
	address := receiverConfig.Listen.AsAddress()
	if address == nil {
		address = net.AnyIP
	}

	if receiverConfig.ReceiveOriginalDestination {
		if mss.SocketSettings == nil {
			mss.SocketSettings = &internet.SocketConfig{}
		}
		if mss.SocketSettings.Tproxy == internet.SocketConfig_Off {
			mss.SocketSettings.Tproxy = internet.SocketConfig_Redirect
		}
		mss.SocketSettings.ReceiveOriginalDestAddress = true
	}
	if pl == nil {
		if net.HasNetwork(nl, net.Network_UNIX) {
			errors.LogDebug(ctx, "creating unix domain socket worker on ", address)

			worker := &dsWorker{
				address:         address,
				proxy:           p,
				stream:          mss,
				tag:             tag,
				dispatcher:      h.mux,
				sniffing:        sniffing,
				uplinkCounter:   uplinkCounter,
				downlinkCounter: downlinkCounter,
				ctx:             ctx,
			}
			h.workers = append(h.workers, worker)
		}
	}
	if pl != nil {
		for _, pr := range pl.Range {
			for port := pr.From; port <= pr.To; port++ {
				if net.HasNetwork(nl, net.Network_TCP) {
					errors.LogDebug(ctx, "creating stream worker on ", address, ":", port)

					worker := &tcpWorker{
						address:         address,
						port:            net.Port(port),
						proxy:           p,
						stream:          mss,
						recvOrigDest:    receiverConfig.ReceiveOriginalDestination,
						tag:             tag,
						dispatcher:      h.mux,
						sniffing:        sniffing,
						uplinkCounter:   uplinkCounter,
						downlinkCounter: downlinkCounter,
						ctx:             ctx,
					}
					h.workers = append(h.workers, worker)
				}

				if net.HasNetwork(nl, net.Network_UDP) {
					worker := &udpWorker{
						tag:             tag,
						proxy:           p,
						address:         address,
						port:            net.Port(port),
						dispatcher:      h.mux,
						sniffing:        sniffing,
						uplinkCounter:   uplinkCounter,
						downlinkCounter: downlinkCounter,
						stream:          mss,
						ctx:             ctx,
					}
					h.workers = append(h.workers, worker)
				}
			}
		}
	}

	return h, nil
}

// Start implements common.Runnable.
func (h *AlwaysOnInboundHandler) Start() error {
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()
	if h.closed {
		return errors.New("handler is closed")
	}
	if h.running {
		return nil
	}
	// for inbound without worker (TUN)
	if run, ok := h.proxy.(common.Runnable); ok {
		if err := run.Start(); err != nil {
			return errors.New("failed to start proxy").Base(err)
		}
	}
	for _, worker := range h.workers {
		if err := worker.Start(); err != nil {
			return err
		}
	}
	h.running = true
	return nil
}

// Close implements common.Closable.
func (h *AlwaysOnInboundHandler) Close() error {
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	h.running = false
	var errs []error
	for _, worker := range h.workers {
		errs = append(errs, worker.Close())
	}
	errs = append(errs, h.mux.Close())
	errs = append(errs, common.Close(h.proxy))
	if err := errors.Combine(errs...); err != nil {
		return errors.New("failed to close all resources").Base(err)
	}
	return nil
}

func (h *AlwaysOnInboundHandler) Tag() string {
	return h.tag
}

func (h *AlwaysOnInboundHandler) GetInbound() proxy.Inbound {
	return h.proxy
}

// ReceiverSettings implements inbound.Handler.
func (h *AlwaysOnInboundHandler) ReceiverSettings() *serial.TypedMessage {
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()
	return serial.ToTypedMessage(h.receiverConfig)
}

// UpdateReceiver changes only sniffing. All socket/protocol settings must match,
// so the listener, user tables and established sessions remain owned by h.
func (h *AlwaysOnInboundHandler) UpdateReceiver(next *proxyman.ReceiverConfig) error {
	request, err := proxyman.BuildSniffingRequest(next.SniffingSettings)
	if err != nil {
		return err
	}
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()
	if h.closed {
		return errors.New("handler is closed")
	}
	oldBase := proto.Clone(h.receiverConfig).(*proxyman.ReceiverConfig)
	nextBase := proto.Clone(next).(*proxyman.ReceiverConfig)
	oldBase.SniffingSettings, nextBase.SniffingSettings = nil, nil
	if !proto.Equal(oldBase, nextBase) {
		return errors.New("listener/transport changes require rebuilding")
	}
	h.receiverConfig = proto.Clone(next).(*proxyman.ReceiverConfig)
	h.sniffing.Store(&request)
	return nil
}

// ProxySettings implements inbound.Handler.
func (h *AlwaysOnInboundHandler) ProxySettings() *serial.TypedMessage {
	if v, ok := h.proxyConfig.(proto.Message); ok {
		return serial.ToTypedMessage(v)
	}
	return nil
}
