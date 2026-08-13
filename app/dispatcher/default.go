package dispatcher

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/routing"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

var errSniffingTimeout = errors.New("timeout on sniffing")

type cachedReader struct {
	sync.Mutex
	reader buf.TimeoutReader // *pipe.Reader、限速包装器(*buf.RateLimitReader/*buf.FairLimitReader) 或 *buf.TimeoutWrapperReader
	cache  buf.MultiBuffer
}

// asTimeoutReader 把任意 Reader 归一成 TimeoutReader。*pipe.Reader 与限速包装器
// (*buf.RateLimitReader / *buf.FairLimitReader) 本身都实现 ReadMultiBufferTimeout，
// 其余类型兜底套 TimeoutWrapperReader。禁止对 outbound.Reader 强转 *pipe.Reader：
// 用户开限速后 Reader 已被包装，强转会 panic 拖崩整个 xray 进程。
func asTimeoutReader(reader buf.Reader) buf.TimeoutReader {
	if tr, ok := reader.(buf.TimeoutReader); ok {
		return tr
	}
	return &buf.TimeoutWrapperReader{Reader: reader}
}

func (r *cachedReader) Cache(b *buf.Buffer, deadline time.Duration) error {
	mb, err := r.reader.ReadMultiBufferTimeout(deadline)
	if err != nil {
		return err
	}
	r.Lock()
	if !mb.IsEmpty() {
		r.cache, _ = buf.MergeMulti(r.cache, mb)
	}
	b.Clear()
	rawBytes := b.Extend(min(r.cache.Len(), b.Cap()))
	n := r.cache.Copy(rawBytes)
	b.Resize(0, int32(n))
	r.Unlock()
	return nil
}

func (r *cachedReader) readInternal() buf.MultiBuffer {
	r.Lock()
	defer r.Unlock()

	if r.cache != nil && !r.cache.IsEmpty() {
		mb := r.cache
		r.cache = nil
		return mb
	}

	return nil
}

func (r *cachedReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb := r.readInternal()
	if mb != nil {
		return mb, nil
	}

	return r.reader.ReadMultiBuffer()
}

func (r *cachedReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	mb := r.readInternal()
	if mb != nil {
		return mb, nil
	}

	return r.reader.ReadMultiBufferTimeout(timeout)
}

func (r *cachedReader) Interrupt() {
	r.Lock()
	if r.cache != nil {
		r.cache = buf.ReleaseMulti(r.cache)
	}
	r.Unlock()
	// 限速包装器可能嵌套在 pipe.Reader 外层，逐层解包才能真正打断底层管道。
	reader := buf.Reader(r.reader)
	for {
		switch v := reader.(type) {
		case *pipe.Reader:
			v.Interrupt()
			return
		case *buf.RateLimitReader:
			reader = v.Reader
		case *buf.FairLimitReader:
			reader = v.Reader
		default:
			return
		}
	}
}

// DefaultDispatcher is a default implementation of Dispatcher.
type DefaultDispatcher struct {
	ohm    outbound.Manager
	router routing.Router
	policy policy.Manager
	stats  stats.Manager
	fdns   dns.FakeDNSEngine
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		d := new(DefaultDispatcher)
		if err := core.RequireFeatures(ctx, func(om outbound.Manager, router routing.Router, pm policy.Manager, sm stats.Manager, dc dns.Client) error {
			core.OptionalFeatures(ctx, func(fdns dns.FakeDNSEngine) {
				d.fdns = fdns
			})
			return d.Init(config.(*Config), om, router, pm, sm)
		}); err != nil {
			return nil, err
		}
		return d, nil
	}))
}

// Init initializes DefaultDispatcher.
func (d *DefaultDispatcher) Init(config *Config, om outbound.Manager, router routing.Router, pm policy.Manager, sm stats.Manager) error {
	d.ohm = om
	d.router = router
	d.policy = pm
	d.stats = sm
	return nil
}

// Type implements common.HasType.
func (*DefaultDispatcher) Type() interface{} {
	return routing.DispatcherType()
}

// Start implements common.Runnable.
func (*DefaultDispatcher) Start() error {
	return nil
}

// Close implements common.Closable.
func (*DefaultDispatcher) Close() error { return nil }

func (d *DefaultDispatcher) getLink(ctx context.Context, destination net.Destination) (*transport.Link, *transport.Link) {
	opt := pipe.OptionsFromContext(ctx)
	uplinkReader, uplinkWriter := pipe.New(opt...)
	downlinkReader, downlinkWriter := pipe.New(opt...)

	inboundLink := &transport.Link{
		Reader: downlinkReader,
		Writer: uplinkWriter,
	}

	outboundLink := &transport.Link{
		Reader: uplinkReader,
		Writer: downlinkWriter,
	}

	sessionInbound := session.InboundFromContext(ctx)
	var user *protocol.MemoryUser
	if sessionInbound != nil {
		user = sessionInbound.User
	}

	if user != nil && len(user.Email) > 0 {
		if limiter, _ := user.RuntimeRateLimiter(buf.NewRateLimiter); limiter != nil {
			// getLink 的两条管道是分开的（不是 WrapLink 那种单条双向 link）：
			//   downlink 管道 = inboundLink.Reader (读) / outboundLink.Writer (写)
			//   uplink   管道 = inboundLink.Writer (写) / outboundLink.Reader (读)
			// （方向由下方 stats 计数器包裹位置佐证：uplink 计数器包 inboundLink.Writer，
			//   downlink 计数器包 outboundLink.Writer。）
			// 每条管道各包【一次】同一个 limiter，读写共享同一用户桶（proto:182「读写共享同一用户桶」）。
			// 此前把 limiter 同时套在 downlink 的两端（inboundLink.Reader + outboundLink.Writer），
			// uplink 管道两端都没套 → 上行(上传)不限速（bug，从 WrapLink 误抄：那里 Reader=uplink/
			// Writer=downlink 是单条 link 的恒等式，在这里的双管道拓扑不成立）。
			// 修正：downlink 读端 + uplink 读端 各套一次。
			inboundLink.Reader = buf.NewRateLimitReaderWithLimiter(ctx, inboundLink.Reader, limiter)   // downlink
			outboundLink.Reader = buf.NewRateLimitReaderWithLimiter(ctx, outboundLink.Reader, limiter) // uplink
		}
		// 节点级公平限速（ipipx 魔改）：套在 per-user 桶之外，双向整形使「节点总出口」生效。
		// 节点公平未开启时 Acquire 返回 nil，wrapper 直通（零开销）。
		// 同上：每条管道包一次，且方向要对——down 喂 downlink 读端，up 喂 uplink 读端。
		if up, down, onBytes, release := protocol.FairScheduler().Acquire(user); up != nil {
			inboundLink.Reader = buf.NewFairLimitReader(ctx, inboundLink.Reader, down, onBytes) // downlink ← down
			outboundLink.Reader = buf.NewFairLimitReader(ctx, outboundLink.Reader, up, onBytes) // uplink ← up
			context.AfterFunc(ctx, release)
		}
		p := d.policy.ForLevel(user.Level)
		if p.Stats.UserUplink {
			name := "user>>>" + user.Email + ">>>traffic>>>uplink"
			if c, _ := d.stats.GetOrRegisterCounter(name); c != nil {
				inboundLink.Writer = &SizeStatWriter{
					Counter: c,
					Writer:  inboundLink.Writer,
				}
			}
		}
		if p.Stats.UserDownlink {
			name := "user>>>" + user.Email + ">>>traffic>>>downlink"
			if c, _ := d.stats.GetOrRegisterCounter(name); c != nil {
				outboundLink.Writer = &SizeStatWriter{
					Counter: c,
					Writer:  outboundLink.Writer,
				}
			}
		}

		if p.Stats.UserSite {
			if site := siteKey(destination); site != "" {
				if c, _ := d.stats.GetOrRegisterCounter(siteCounterName(user.Email, site, "uplink")); c != nil {
					inboundLink.Writer = &SizeStatWriter{Counter: c, Writer: inboundLink.Writer}
				}
				if c, _ := d.stats.GetOrRegisterCounter(siteCounterName(user.Email, site, "downlink")); c != nil {
					outboundLink.Writer = &SizeStatWriter{Counter: c, Writer: outboundLink.Writer}
				}
			}
		}

		if p.Stats.UserOnline {
			trackOnlineIP(ctx, d.stats, user.Email, sessionInbound.Source.Address.String())
		}
	}

	return inboundLink, outboundLink
}

// siteKey returns the per-destination "site" label for traffic aggregation: the
// requested domain when present, otherwise the destination IP. For mixed/socks/
// http this is the user's requested host, which is exactly the site we bill on.
func siteKey(destination net.Destination) string {
	addr := destination.Address
	if addr == nil {
		return ""
	}
	return addr.String()
}

// siteCounterName is the stats key node-agent's CollectSiteTraffic parses. The
// "site>>>{domain}" segment sits alongside the per-user "traffic>>>" counters so
// one QueryStats(pattern="user>>>") read collects both.
func siteCounterName(email, site, dir string) string {
	return "user>>>" + email + ">>>site>>>" + site + ">>>traffic>>>" + dir
}

func WrapLink(ctx context.Context, policyManager policy.Manager, statsManager stats.Manager, link *transport.Link) *transport.Link {
	sessionInbound := session.InboundFromContext(ctx)
	var user *protocol.MemoryUser
	if sessionInbound != nil {
		user = sessionInbound.User
	}

	timeoutReader := &buf.TimeoutWrapperReader{Reader: link.Reader}
	link.Reader = timeoutReader

	if user != nil && len(user.Email) > 0 {
		if limiter, _ := user.RuntimeRateLimiter(buf.NewRateLimiter); limiter != nil {
			link.Reader = buf.NewRateLimitReaderWithLimiter(ctx, link.Reader, limiter)
			link.Writer = buf.NewRateLimitWriterWithLimiter(ctx, link.Writer, limiter)
		}
		// 节点级公平限速（ipipx 魔改）：套在 per-user 桶之外，双向整形使「节点总出口」生效。
		if up, down, onBytes, release := protocol.FairScheduler().Acquire(user); up != nil {
			link.Reader = buf.NewFairLimitReader(ctx, link.Reader, up, onBytes)
			link.Writer = buf.NewFairLimitWriter(ctx, link.Writer, down, onBytes)
			context.AfterFunc(ctx, release)
		}
		p := policyManager.ForLevel(user.Level)
		if p.Stats.UserUplink {
			name := "user>>>" + user.Email + ">>>traffic>>>uplink"
			if c, _ := statsManager.GetOrRegisterCounter(name); c != nil {
				timeoutReader.Counter = c
			}
		}
		if p.Stats.UserDownlink {
			name := "user>>>" + user.Email + ">>>traffic>>>downlink"
			if c, _ := statsManager.GetOrRegisterCounter(name); c != nil {
				link.Writer = &SizeStatWriter{
					Counter: c,
					Writer:  link.Writer,
				}
			}
		}
		if p.Stats.UserSite {
			if site := siteKey(outboundTarget(ctx)); site != "" {
				if c, _ := statsManager.GetOrRegisterCounter(siteCounterName(user.Email, site, "uplink")); c != nil {
					// timeoutReader already counts uplink bytes when its Counter is
					// set; chain a site reader so both per-user and per-site count.
					link.Reader = &siteReadCounter{Reader: link.Reader, counter: c}
				}
				if c, _ := statsManager.GetOrRegisterCounter(siteCounterName(user.Email, site, "downlink")); c != nil {
					link.Writer = &SizeStatWriter{Counter: c, Writer: link.Writer}
				}
			}
		}

		if p.Stats.UserOnline {
			trackOnlineIP(ctx, statsManager, user.Email, sessionInbound.Source.Address.String())
		}
	}

	return link
}

// outboundTarget returns the resolved destination for the current connection, set
// by Dispatch/DispatchLink before WrapLink runs.
func outboundTarget(ctx context.Context) net.Destination {
	obs := session.OutboundsFromContext(ctx)
	if len(obs) == 0 {
		return net.Destination{}
	}
	return obs[len(obs)-1].Target
}

// enforceConnLimit applies the protocol-agnostic per-user connection cap. It is
// the single enforcement point for conn_limit across every inbound (mixed/socks/
// http, vless, shadowsocks, trojan): the dispatcher already holds the session
// *MemoryUser, so no proxy needs its own counter. The reserved slot is released
// when the connection context is cancelled (same lifecycle trackOnlineIP uses).
func enforceConnLimit(ctx context.Context) error {
	sessionInbound := session.InboundFromContext(ctx)
	if sessionInbound == nil || sessionInbound.User == nil {
		return nil
	}
	release, ok := sessionInbound.User.AcquireRuntimeConnection()
	if !ok {
		return errors.New("user ", sessionInbound.User.Email, " connection limit exceeded").AtWarning()
	}
	context.AfterFunc(ctx, release)
	return nil
}

func trackOnlineIP(ctx context.Context, sm stats.Manager, email, ip string) {
	name := "user>>>" + email + ">>>online"
	if om, _ := sm.GetOrRegisterOnlineMap(name); om != nil {
		om.AddIP(ip)
		context.AfterFunc(ctx, func() { om.RemoveIP(ip) })
	}
}

func (d *DefaultDispatcher) shouldOverride(ctx context.Context, result SniffResult, request session.SniffingRequest, destination net.Destination) bool {
	domain := result.Domain()
	if domain == "" {
		return false
	}
	if request.ExcludeForDomain != nil && request.ExcludeForDomain.MatchAny(strings.ToLower(domain)) {
		return false
	}
	if request.ExcludeForIP != nil && destination.Address.Family().IsIP() && request.ExcludeForIP.Match(destination.Address.IP()) {
		return false
	}
	protocolString := result.Protocol()
	if resComp, ok := result.(SnifferResultComposite); ok {
		protocolString = resComp.ProtocolForDomainResult()
	}
	for _, p := range request.OverrideDestinationForProtocol {
		if strings.HasPrefix(protocolString, p) || strings.HasPrefix(p, protocolString) {
			return true
		}
		if fkr0, ok := d.fdns.(dns.FakeDNSEngineRev0); ok && protocolString != "bittorrent" && p == "fakedns" &&
			fkr0.IsIPInIPPool(destination.Address) {
			errors.LogInfo(ctx, "Using sniffer ", protocolString, " since the fake DNS missed")
			return true
		}
		if resultSubset, ok := result.(SnifferIsProtoSubsetOf); ok {
			if resultSubset.IsProtoSubsetOf(p) {
				return true
			}
		}
	}

	return false
}

// Dispatch implements routing.Dispatcher.
func (d *DefaultDispatcher) Dispatch(ctx context.Context, destination net.Destination) (*transport.Link, error) {
	if !destination.IsValid() {
		panic("Dispatcher: Invalid destination.")
	}
	if err := enforceConnLimit(ctx); err != nil {
		return nil, err
	}
	outbounds := session.OutboundsFromContext(ctx)
	if len(outbounds) == 0 {
		outbounds = []*session.Outbound{{}}
		ctx = session.ContextWithOutbounds(ctx, outbounds)
	}
	ob := outbounds[len(outbounds)-1]
	ob.OriginalTarget = destination
	ob.Target = destination
	content := session.ContentFromContext(ctx)
	if content == nil {
		content = new(session.Content)
		ctx = session.ContextWithContent(ctx, content)
	}

	sniffingRequest := content.SniffingRequest
	inbound, outbound := d.getLink(ctx, destination)
	if !sniffingRequest.Enabled {
		go d.routedDispatch(ctx, outbound, destination)
	} else {
		go func() {
			cReader := &cachedReader{
				reader: asTimeoutReader(outbound.Reader),
			}
			outbound.Reader = cReader
			result, err := sniffer(ctx, cReader, sniffingRequest.MetadataOnly, destination.Network)
			if err == nil {
				content.Protocol = result.Protocol()
			}
			if err == nil && d.shouldOverride(ctx, result, sniffingRequest, destination) {
				domain := result.Domain()
				errors.LogInfo(ctx, "sniffed domain: ", domain)
				destination.Address = net.ParseAddress(domain)
				protocol := result.Protocol()
				if resComp, ok := result.(SnifferResultComposite); ok {
					protocol = resComp.ProtocolForDomainResult()
				}
				isFakeIP := false
				if fkr0, ok := d.fdns.(dns.FakeDNSEngineRev0); ok && fkr0.IsIPInIPPool(ob.Target.Address) {
					isFakeIP = true
				}
				if sniffingRequest.RouteOnly && protocol != "fakedns" && protocol != "fakedns+others" && !isFakeIP {
					ob.RouteTarget = destination
				} else {
					ob.Target = destination
				}
			}
			d.routedDispatch(ctx, outbound, destination)
		}()
	}
	return inbound, nil
}

// DispatchLink implements routing.Dispatcher.
func (d *DefaultDispatcher) DispatchLink(ctx context.Context, destination net.Destination, outbound *transport.Link) error {
	if !destination.IsValid() {
		return errors.New("Dispatcher: Invalid destination.")
	}
	if err := enforceConnLimit(ctx); err != nil {
		return err
	}
	outbounds := session.OutboundsFromContext(ctx)
	if len(outbounds) == 0 {
		outbounds = []*session.Outbound{{}}
		ctx = session.ContextWithOutbounds(ctx, outbounds)
	}
	ob := outbounds[len(outbounds)-1]
	ob.OriginalTarget = destination
	ob.Target = destination
	content := session.ContentFromContext(ctx)
	if content == nil {
		content = new(session.Content)
		ctx = session.ContextWithContent(ctx, content)
	}
	outbound = WrapLink(ctx, d.policy, d.stats, outbound)
	sniffingRequest := content.SniffingRequest
	if !sniffingRequest.Enabled {
		d.routedDispatch(ctx, outbound, destination)
	} else {
		cReader := &cachedReader{
			reader: asTimeoutReader(outbound.Reader),
		}
		outbound.Reader = cReader
		result, err := sniffer(ctx, cReader, sniffingRequest.MetadataOnly, destination.Network)
		if err == nil {
			content.Protocol = result.Protocol()
		}
		if err == nil && d.shouldOverride(ctx, result, sniffingRequest, destination) {
			domain := result.Domain()
			errors.LogInfo(ctx, "sniffed domain: ", domain)
			destination.Address = net.ParseAddress(domain)
			protocol := result.Protocol()
			if resComp, ok := result.(SnifferResultComposite); ok {
				protocol = resComp.ProtocolForDomainResult()
			}
			isFakeIP := false
			if fkr0, ok := d.fdns.(dns.FakeDNSEngineRev0); ok && fkr0.IsIPInIPPool(ob.Target.Address) {
				isFakeIP = true
			}
			if sniffingRequest.RouteOnly && protocol != "fakedns" && protocol != "fakedns+others" && !isFakeIP {
				ob.RouteTarget = destination
			} else {
				ob.Target = destination
			}
		}
		d.routedDispatch(ctx, outbound, destination)
	}

	return nil
}

func sniffer(ctx context.Context, cReader *cachedReader, metadataOnly bool, network net.Network) (SniffResult, error) {
	payload := buf.NewWithSize(32767)
	defer payload.Release()

	sniffer := NewSniffer(ctx)

	metaresult, metadataErr := sniffer.SniffMetadata(ctx)

	if metadataOnly {
		return metaresult, metadataErr
	}

	contentResult, contentErr := func() (SniffResult, error) {
		cacheDeadline := 200 * time.Millisecond
		totalAttempt := 0
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				cachingStartingTimeStamp := time.Now()
				err := cReader.Cache(payload, cacheDeadline)
				if err != nil {
					return nil, err
				}
				cachingTimeElapsed := time.Since(cachingStartingTimeStamp)
				cacheDeadline -= cachingTimeElapsed

				if !payload.IsEmpty() {
					result, err := sniffer.Sniff(ctx, payload.Bytes(), network)
					switch err {
					case common.ErrNoClue: // No Clue: protocol not matches, and sniffer cannot determine whether there will be a match or not
						totalAttempt++
					case protocol.ErrProtoNeedMoreData: // Protocol Need More Data: protocol matches, but need more data to complete sniffing
						// in this case, do not add totalAttempt(allow to read until timeout)
					default:
						return result, err
					}
				} else {
					totalAttempt++
				}
				if totalAttempt >= 2 || cacheDeadline <= 0 {
					return nil, errSniffingTimeout
				}
			}
		}
	}()
	if contentErr != nil && metadataErr == nil {
		return metaresult, nil
	}
	if contentErr == nil && metadataErr == nil {
		return CompositeResult(metaresult, contentResult), nil
	}
	return contentResult, contentErr
}

func (d *DefaultDispatcher) routedDispatch(ctx context.Context, link *transport.Link, destination net.Destination) {
	outbounds := session.OutboundsFromContext(ctx)
	ob := outbounds[len(outbounds)-1]

	var handler outbound.Handler

	routingLink := routing_session.AsRoutingContext(ctx)
	inTag := routingLink.GetInboundTag()
	isPickRoute := 0
	if forcedOutboundTag := session.GetForcedOutboundTagFromContext(ctx); forcedOutboundTag != "" {
		ctx = session.SetForcedOutboundTagToContext(ctx, "")
		if h := d.ohm.GetHandler(forcedOutboundTag); h != nil {
			isPickRoute = 1
			errors.LogInfo(ctx, "taking platform initialized detour [", forcedOutboundTag, "] for [", destination, "]")
			handler = h
		} else {
			errors.LogError(ctx, "non existing tag for platform initialized detour: ", forcedOutboundTag)
			common.Close(link.Writer)
			common.Interrupt(link.Reader)
			return
		}
	} else if d.router != nil {
		if route, err := d.router.PickRoute(routingLink); err == nil {
			outTag := route.GetOutboundTag()
			if h := d.ohm.GetHandler(outTag); h != nil {
				isPickRoute = 2
				if route.GetRuleTag() == "" {
					errors.LogInfo(ctx, "taking detour [", outTag, "] for [", destination, "]")
				} else {
					errors.LogInfo(ctx, "Hit route rule: [", route.GetRuleTag(), "] so taking detour [", outTag, "] for [", destination, "]")
				}
				handler = h
			} else {
				errors.LogWarning(ctx, "non existing outTag: ", outTag)
				common.Close(link.Writer)
				common.Interrupt(link.Reader)
				return // DO NOT CHANGE: the traffic shouldn't be processed by default outbound if the specified outbound tag doesn't exist (yet), e.g., VLESS Reverse Proxy
			}
		} else {
			errors.LogInfo(ctx, "default route for ", destination)
		}
	}

	if handler == nil {
		handler = d.ohm.GetDefaultHandler()
	}

	if handler == nil {
		errors.LogInfo(ctx, "default outbound handler not exist")
		common.Close(link.Writer)
		common.Interrupt(link.Reader)
		return
	}

	ob.Tag = handler.Tag()
	if accessMessage := log.AccessMessageFromContext(ctx); accessMessage != nil {
		if tag := handler.Tag(); tag != "" {
			if inTag == "" {
				accessMessage.Detour = tag
			} else if isPickRoute == 1 {
				accessMessage.Detour = inTag + " ==> " + tag
			} else if isPickRoute == 2 {
				accessMessage.Detour = inTag + " -> " + tag
			} else {
				accessMessage.Detour = inTag + " >> " + tag
			}
		}
		log.Record(accessMessage)
	}

	handler.Dispatch(ctx, link)

	// ipipx 旁路：连接生命周期结束后记一条访问（唯一 emit 点）。此刻 freedom 已回填 DialedRemoteAddr，
	// destIP 可信；域名取自 ob.Target/OriginalTarget。覆盖 freedom 真出口 + blackhole 禁陆，每连接恰一条。
	emitAccessForOutbound(ctx, ob, time.Now().Unix())
}
