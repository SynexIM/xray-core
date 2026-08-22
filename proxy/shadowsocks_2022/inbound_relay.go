package shadowsocks_2022

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/sagernet/sing-shadowsocks/shadowaead_2022"
	C "github.com/sagernet/sing/common"
	A "github.com/sagernet/sing/common/auth"
	B "github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal"
	"github.com/xtls/xray-core/common/singbridge"
	"github.com/xtls/xray-core/common/uuid"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func init() {
	common.Must(common.RegisterConfig((*RelayServerConfig)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return NewRelayServer(ctx, config.(*RelayServerConfig))
	}))
}

type RelayInbound struct {
	networks     []net.Network
	destinations []*RelayDestination
	// users[i] 对应 destinations[i]，**进程内长期存活、一个 destination 一个**。
	//
	// 这一层不是为了好看：每用户限速的桶是按 *MemoryUser 指针缓存的
	// （见 protocol.RuntimeRateLimiters），每来一条连接就现造一个 MemoryUser 的话，
	// 每条连接都会拿到一套自己的满桶——限速看着「配上了」，实际开 10 条连接就是 10 倍。
	// 所以这里跟 MultiUserInbound 一样，把 MemoryUser 提前建好并复用。
	users   []*protocol.MemoryUser
	service *shadowaead_2022.RelayService[int]
}

func NewRelayServer(ctx context.Context, config *RelayServerConfig) (*RelayInbound, error) {
	networks := config.Network
	if len(networks) == 0 {
		networks = []net.Network{
			net.Network_TCP,
			net.Network_UDP,
		}
	}
	inbound := &RelayInbound{
		networks:     networks,
		destinations: config.Destinations,
	}
	if !C.Contains(shadowaead_2022.List, config.Method) || !strings.Contains(config.Method, "aes") {
		return nil, errors.New("unsupported method ", config.Method)
	}
	service, err := shadowaead_2022.NewRelayServiceWithPassword[int](config.Method, config.Key, 500, inbound)
	if err != nil {
		return nil, errors.New("create service").Base(err)
	}

	inbound.users = make([]*protocol.MemoryUser, len(config.Destinations))
	for i, destination := range config.Destinations {
		if destination.Email == "" {
			u := uuid.New()
			destination.Email = "unnamed-destination-" + strconv.Itoa(i) + "-" + u.String()
		}
		inbound.users[i] = destination.ToMemoryUser()
	}
	err = service.UpdateUsersWithPasswords(
		C.MapIndexed(config.Destinations, func(index int, it *RelayDestination) int { return index }),
		C.Map(config.Destinations, func(it *RelayDestination) string { return it.Key }),
		C.Map(config.Destinations, func(it *RelayDestination) M.Socksaddr {
			return singbridge.ToSocksaddr(net.Destination{
				Address: it.Address.AsAddress(),
				Port:    net.Port(it.Port),
			})
		}),
	)
	if err != nil {
		return nil, errors.New("create service").Base(err)
	}
	inbound.service = service
	return inbound, nil
}

// ToMemoryUser 把一个 relay destination 翻成 dispatcher 认识的用户。
// 限速字段直接搬过去——dispatcher 只认 *MemoryUser，认了就跟别的协议一视同仁。
//
// 导出是为了让 infra/conf 的全协议限速矩阵能拿同一段映射做断言：
// 矩阵和线上走的是同一个函数，这里漏搬一个字段，矩阵就会红。
func (d *RelayDestination) ToMemoryUser() *protocol.MemoryUser {
	return &protocol.MemoryUser{
		Email:               d.Email,
		Level:               uint32(d.Level),
		BandwidthBps:        d.BandwidthBps,
		ConnLimit:           d.ConnLimit,
		CommittedBps:        d.CommittedBps,
		CommittedBurstBytes: d.CommittedBurstBytes,
		Class:               d.Class,
	}
}

// userAt 取第 n 个 destination 对应的长期 MemoryUser。索引由 sing 的
// RelayService 依 PSK 认证结果给出，越界只可能是内部不一致——兜底返回 nil
// 比 panic 拖崩整个 xray 进程好。
func (i *RelayInbound) userAt(n int) *protocol.MemoryUser {
	if n < 0 || n >= len(i.users) {
		return nil
	}
	return i.users[n]
}

func (i *RelayInbound) Network() []net.Network {
	return i.networks
}

func (i *RelayInbound) Process(ctx context.Context, network net.Network, connection stat.Connection, dispatcher routing.Dispatcher) error {
	inbound := session.InboundFromContext(ctx)
	inbound.Name = "shadowsocks-2022-relay"
	inbound.CanSpliceCopy = 3

	var metadata M.Metadata
	if inbound.Source.IsValid() {
		metadata.Source = M.ParseSocksaddr(inbound.Source.NetAddr())
	}

	ctx = session.ContextWithDispatcher(ctx, dispatcher)

	if network == net.Network_TCP {
		return singbridge.ReturnError(i.service.NewConnection(ctx, connection, metadata))
	} else {
		reader := buf.NewReader(connection)
		pc := &natPacketConn{connection}
		for {
			mb, err := reader.ReadMultiBuffer()
			if err != nil {
				buf.ReleaseMulti(mb)
				return singbridge.ReturnError(err)
			}
			for _, buffer := range mb {
				packet := B.As(buffer.Bytes()).ToOwned()
				buffer.Release()
				err = i.service.NewPacket(ctx, pc, packet, metadata)
				if err != nil {
					packet.Release()
					buf.ReleaseMulti(mb)
					return err
				}
			}
		}
	}
}

func (i *RelayInbound) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	inbound := session.InboundFromContext(ctx)
	userInt, _ := A.UserFromContext[int](ctx)
	user := i.userAt(userInt)
	if user == nil {
		return errors.New("relay destination index out of range: ", userInt)
	}
	inbound.User = user
	ctx = log.ContextWithAccessMessage(ctx, &log.AccessMessage{
		From:   metadata.Source,
		To:     metadata.Destination,
		Status: log.AccessAccepted,
		Email:  user.Email,
	})
	errors.LogInfo(ctx, "tunnelling request to tcp:", metadata.Destination)
	dispatcher := session.DispatcherFromContext(ctx)
	destination, err := singbridge.ToDestination(metadata.Destination, net.Network_TCP)
	if err != nil {
		return err
	}
	link, err := dispatcher.Dispatch(ctx, destination)
	if err != nil {
		return err
	}
	return singbridge.CopyConn(ctx, nil, link, conn)
}

func (i *RelayInbound) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	inbound := session.InboundFromContext(ctx)
	userInt, _ := A.UserFromContext[int](ctx)
	user := i.userAt(userInt)
	if user == nil {
		return errors.New("relay destination index out of range: ", userInt)
	}
	inbound.User = user
	ctx = log.ContextWithAccessMessage(ctx, &log.AccessMessage{
		From:   metadata.Source,
		To:     metadata.Destination,
		Status: log.AccessAccepted,
		Email:  user.Email,
	})
	errors.LogInfo(ctx, "tunnelling request to udp:", metadata.Destination)
	dispatcher := session.DispatcherFromContext(ctx)
	destination, err := singbridge.ToDestination(metadata.Destination, net.Network_UDP)
	if err != nil {
		return err
	}
	link, err := dispatcher.Dispatch(ctx, destination)
	if err != nil {
		return err
	}
	outConn := &singbridge.PacketConnWrapper{
		Reader: link.Reader,
		Writer: link.Writer,
		Dest:   destination,
		T: signal.CancelAfterInactivity(ctx, func() {
			common.Interrupt(link.Reader)
		}, 300*time.Second),
	}
	return bufio.CopyPacketConn(ctx, conn, outConn)
}

func (i *RelayInbound) NewError(ctx context.Context, err error) {
	if E.IsClosed(err) {
		return
	}
	errors.LogWarning(ctx, err.Error())
}
