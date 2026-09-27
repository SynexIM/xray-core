package dispatcher

import (
	"context"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
)

// AccessEvent 是一次出站连接的访问事件（ipipx 旁路：供 node-agent 聚合成 AccessReport）。
// 它在连接建立、目标确定时发出，此刻还不知道字节数——up/down 由后续 stats 路径填，hook 这里不带。
type AccessEvent struct {
	Email    string // 归属 user（session inbound 的 User.Email，空则匿名/无 user）
	Domain   string // 嗅探域名（TLS SNI / HTTP Host）；纯 IP 直连为空
	DestIP   string // 目标 IP（原始出口目标地址；嗅探出域名后仍是连接前的 destIP）
	Protocol string // 嗅探出的协议（tls/http/…）；非嗅探为空
	UnixSec  int64  // 事件发生时刻（unix 秒）
}

// accessHook 是包级访问事件回调，nil 时（默认）emit 零开销、不影响转发路径。
// 由 node-agent 通过 RegisterAccessHook 注入；hook 内部须自己用有界 channel 非阻塞处理。
var accessHook func(AccessEvent)

// RegisterAccessHook 注入访问事件回调。非线程安全的一次性注册：node-agent 在 xray 起转发前调用。
func RegisterAccessHook(fn func(AccessEvent)) {
	accessHook = fn
}

// emitAccess 把一次访问事件投给已注册的 hook。hook 为 nil 直接 return（零开销）。
// 约定：emit 本身只做一次函数调用，绝不阻塞、绝不影响转发——背压/丢弃是 hook 内部有界 channel 的事。
func emitAccess(ev AccessEvent) {
	h := accessHook
	if h == nil {
		return
	}
	h(ev)
}

// accessAddrIP 取一个 net.Address 的 IP 字符串；非 IP（域名/nil）返回空。
func accessAddrIP(addr net.Address) string {
	if addr == nil || !addr.Family().IsIP() {
		return ""
	}
	return addr.IP().String()
}

// accessDomainOf 取一个目标的域名；非域名（IP/nil）返回空。
func accessDomainOf(dest net.Destination) string {
	addr := dest.Address
	if addr == nil || !addr.Family().IsDomain() {
		return ""
	}
	return addr.Domain()
}

// emitAccessForOutbound 在出站连接生命周期结束后（routedDispatch 末尾）发一条访问事件。
//
// 这是唯一的访问 emit 点（覆盖 freedom 真出口 + blackhole 禁陆，每连接恰一条，无重复）：
//   - domain：嗅探/客户端目标里的域名（ob.Target 优先，回退 ob.OriginalTarget）；纯 IP 连接为空。
//   - destIP：真正拨通的对端 IP（ob.DialedRemoteAddr，由 freedom 解析域名后拨通回填）；
//     回退到原始/当前目标里的 IP（客户端直接送 IP 时）。blackhole 禁陆无真连接 → 域名有、IP 空（不造假）。
//   - protocol：嗅探出的协议（content.Protocol）。
//
// hook 未注册时整体零开销。无 email（匿名/无 user）由聚合器侧丢弃。
func emitAccessForOutbound(ctx context.Context, ob *session.Outbound, unixSec int64) {
	if accessHook == nil || ob == nil {
		return
	}
	var email string
	if sb := session.InboundFromContext(ctx); sb != nil && sb.User != nil {
		email = sb.User.Email
	}
	domain := accessDomainOf(ob.Target)
	if domain == "" {
		domain = accessDomainOf(ob.OriginalTarget)
	}
	destIP := accessAddrIP(ob.DialedRemoteAddr)
	if destIP == "" {
		destIP = accessAddrIP(ob.OriginalTarget.Address)
	}
	if destIP == "" {
		destIP = accessAddrIP(ob.Target.Address)
	}
	var protocolStr string
	if content := session.ContentFromContext(ctx); content != nil {
		protocolStr = content.Protocol
	}
	emitAccess(AccessEvent{
		Email:    email,
		Domain:   domain,
		DestIP:   destIP,
		Protocol: protocolStr,
		UnixSec:  unixSec,
	})
}
