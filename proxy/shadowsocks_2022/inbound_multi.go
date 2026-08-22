package shadowsocks_2022

import (
	"context"
	"encoding/base64"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	common.Must(common.RegisterConfig((*MultiUserServerConfig)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return NewMultiServer(ctx, config.(*MultiUserServerConfig))
	}))
}

// MultiUserInbound 的用户表：一份 slice（sing 的 EIH 表按下标索引它）+ 一份
// email→下标的索引。
//
// 为什么要索引：目标是 5 万实例/单节点组，AddUser/RemoveUser/GetUser 原来都是
// 线性扫 email，管理路径 O(N)。索引把它压成 O(1)。
//
// 为什么还要批量入口：真正贵的不是扫 email，是 sing 的 EIH 表**整份重建**
// （UpdateUsersWithPasswords 对全表做 base64 解码 + blake3 + AES 派生，上游自己
// 在下面留了注释承认这里性能不行）。逐个增删 5000 个客户 = 重建 5000 次全表。
// AddUsers/RemoveUsers 把一批合成一次重建，并且只锁一次。
type MultiUserInbound struct {
	sync.Mutex
	networks []net.Network
	users    []*protocol.MemoryUser
	// index 是 strings.ToLower(email) → users 下标。
	index   map[string]int
	service *shadowaead_2022.MultiService[int]
}

func NewMultiServer(ctx context.Context, config *MultiUserServerConfig) (*MultiUserInbound, error) {
	networks := config.Network
	if len(networks) == 0 {
		networks = []net.Network{
			net.Network_TCP,
			net.Network_UDP,
		}
	}
	memUsers := []*protocol.MemoryUser{}
	for i, user := range config.Users {
		if user.Email == "" {
			u := uuid.New()
			user.Email = "unnamed-user-" + strconv.Itoa(i) + "-" + u.String()
		}
		u, err := user.ToMemoryUser()
		if err != nil {
			return nil, errors.New("failed to get shadowsocks user").Base(err).AtError()
		}
		memUsers = append(memUsers, u)
	}

	inbound := &MultiUserInbound{
		networks: networks,
		users:    memUsers,
		index:    make(map[string]int, len(memUsers)),
	}
	for i, u := range memUsers {
		inbound.index[strings.ToLower(u.Email)] = i
	}
	if config.Key == "" {
		return nil, errors.New("missing key")
	}
	psk, err := base64.StdEncoding.DecodeString(config.Key)
	if err != nil {
		return nil, errors.New("parse config").Base(err)
	}
	service, err := shadowaead_2022.NewMultiService[int](config.Method, psk, 500, inbound, nil)
	if err != nil {
		return nil, errors.New("create service").Base(err)
	}
	err = service.UpdateUsersWithPasswords(
		C.MapIndexed(memUsers, func(index int, it *protocol.MemoryUser) int { return index }),
		C.Map(memUsers, func(it *protocol.MemoryUser) string { return it.Account.(*MemoryAccount).Key }),
	)
	if err != nil {
		return nil, errors.New("create service").Base(err)
	}

	inbound.service = service
	return inbound, nil
}

// AddUser implements proxy.UserManager.AddUser().
func (i *MultiUserInbound) AddUser(ctx context.Context, u *protocol.MemoryUser) error {
	return i.AddUsers(ctx, []*protocol.MemoryUser{u})
}

// AddUsers implements proxy.BatchUserManager.AddUsers()：一批客户一次入表、
// 一次 EIH 重建、一次锁。5000 个客户的月度换手不该重建 5000 次全表。
//
// 原子：批里有一个 email 撞车就整批不生效，不留半截状态——半截生效的下发会让
// 面板与节点的 configHash 对不上，而那种漂移要人肉去查。
func (i *MultiUserInbound) AddUsers(ctx context.Context, users []*protocol.MemoryUser) error {
	if len(users) == 0 {
		return nil
	}

	i.Lock()
	defer i.Unlock()

	seen := make(map[string]struct{}, len(users))
	for _, u := range users {
		if u.Email == "" {
			continue
		}
		key := strings.ToLower(u.Email)
		if _, exists := i.index[key]; exists {
			return errors.New("User ", u.Email, " already exists.")
		}
		if _, dup := seen[key]; dup {
			return errors.New("User ", u.Email, " appears twice in the same batch.")
		}
		seen[key] = struct{}{}
	}

	for _, u := range users {
		i.index[strings.ToLower(u.Email)] = len(i.users)
		i.users = append(i.users, u)
	}
	return i.syncService()
}

// RemoveUser implements proxy.UserManager.RemoveUser().
func (i *MultiUserInbound) RemoveUser(ctx context.Context, email string) error {
	return i.RemoveUsers(ctx, []string{email})
}

// RemoveUsers implements proxy.BatchUserManager.RemoveUsers()：同 AddUsers，
// 一批一次重建、一次锁、全有或全无。
func (i *MultiUserInbound) RemoveUsers(ctx context.Context, emails []string) error {
	if len(emails) == 0 {
		return nil
	}

	i.Lock()
	defer i.Unlock()

	idxs := make([]int, 0, len(emails))
	seen := make(map[string]struct{}, len(emails))
	for _, email := range emails {
		if email == "" {
			return errors.New("Email must not be empty.")
		}
		key := strings.ToLower(email)
		if _, dup := seen[key]; dup {
			return errors.New("User ", email, " appears twice in the same batch.")
		}
		seen[key] = struct{}{}
		idx, ok := i.index[key]
		if !ok {
			return errors.New("User ", email, " not found.")
		}
		idxs = append(idxs, idx)
	}

	// 从大到小删，swap-with-last 才不会把还没处理的下标搅乱。
	sort.Sort(sort.Reverse(sort.IntSlice(idxs)))
	for _, idx := range idxs {
		last := len(i.users) - 1
		delete(i.index, strings.ToLower(i.users[idx].Email))
		if idx != last {
			i.users[idx] = i.users[last]
			i.index[strings.ToLower(i.users[idx].Email)] = idx
		}
		i.users[last] = nil
		i.users = i.users[:last]
	}
	return i.syncService()
}

// syncService 把用户表推给 sing 的 EIH 表。调用方必须已持锁。
//
// 整份重建是 sing-shadowsocks 的 API 决定的：MultiService 只暴露 UpdateUsers，
// uPSK/uPSKHash/uCipher 三张表都是包内私有，没有增量入口。所以这里能做的是
// **把重建次数压到每批一次**，而不是每个客户一次。
func (i *MultiUserInbound) syncService() error {
	return i.service.UpdateUsersWithPasswords(
		C.MapIndexed(i.users, func(index int, it *protocol.MemoryUser) int { return index }),
		C.Map(i.users, func(it *protocol.MemoryUser) string { return it.Account.(*MemoryAccount).Key }),
	)
}

// GetUser implements proxy.UserManager.GetUser().
func (i *MultiUserInbound) GetUser(ctx context.Context, email string) *protocol.MemoryUser {
	if email == "" {
		return nil
	}

	i.Lock()
	defer i.Unlock()

	if idx, ok := i.index[strings.ToLower(email)]; ok {
		return i.users[idx]
	}
	return nil
}

// GetUsers implements proxy.UserManager.GetUsers().
func (i *MultiUserInbound) GetUsers(ctx context.Context) []*protocol.MemoryUser {
	i.Lock()
	defer i.Unlock()
	dst := make([]*protocol.MemoryUser, len(i.users))
	copy(dst, i.users)
	return dst
}

// GetUsersCount implements proxy.UserManager.GetUsersCount().
func (i *MultiUserInbound) GetUsersCount(context.Context) int64 {
	i.Lock()
	defer i.Unlock()
	return int64(len(i.users))
}

func (i *MultiUserInbound) Network() []net.Network {
	return i.networks
}

func (i *MultiUserInbound) Process(ctx context.Context, network net.Network, connection stat.Connection, dispatcher routing.Dispatcher) error {
	inbound := session.InboundFromContext(ctx)
	inbound.Name = "shadowsocks-2022-multi"
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

func (i *MultiUserInbound) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	inbound := session.InboundFromContext(ctx)
	userInt, _ := A.UserFromContext[int](ctx)
	user := i.users[userInt]
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
	return singbridge.CopyConn(ctx, conn, link, conn)
}

func (i *MultiUserInbound) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	inbound := session.InboundFromContext(ctx)
	userInt, _ := A.UserFromContext[int](ctx)
	user := i.users[userInt]
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

func (i *MultiUserInbound) NewError(ctx context.Context, err error) {
	if E.IsClosed(err) {
		return
	}
	errors.LogWarning(ctx, err.Error())
}
