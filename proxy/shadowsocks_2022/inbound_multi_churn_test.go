package shadowsocks_2022

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
)

// 5 万实例/单节点组的目标下，客户换手是天天发生的事（到期释放、续费、改配）。
// 这个文件量的是那件事到底有多贵：**换 5000 个客户期间，认证路径被锁住多久**。
//
// SS2022 的 EIH 表在 sing-shadowsocks 里是整份重建的（UpdateUsers 每次对全表做
// blake3 + AES 派生），上游自己在 inbound_multi.go 里留了注释承认这里性能不行。
// 逐个增删 = 每个客户重建一次全表 = O(N×M)。批量入口把它压成一次重建。

const churnPSK = "IdG0eY+zbGDpTEBGKcCSXpuMXNiPUFcbZTHDWbBGb5w="

func churnUser(i int) *protocol.MemoryUser {
	key := make([]byte, 16)
	rand.Read(key)
	return &protocol.MemoryUser{
		Email:   "u" + strconv.Itoa(i) + "@churn.test",
		Account: &MemoryAccount{Key: base64.StdEncoding.EncodeToString(key)},
	}
}

func newChurnInbound(tb testing.TB, base int) *MultiUserInbound {
	tb.Helper()
	users := make([]*protocol.User, 0, base)
	for i := 0; i < base; i++ {
		m := churnUser(i)
		users = append(users, &protocol.User{
			Email:   m.Email,
			Account: serial.ToTypedMessage(&Account{Key: m.Account.(*MemoryAccount).Key}),
		})
	}
	inbound, err := NewMultiServer(context.Background(), &MultiUserServerConfig{
		Method: "2022-blake3-aes-128-gcm",
		Key:    churnPSK,
		Users:  users,
	})
	if err != nil {
		tb.Fatal(err)
	}
	return inbound
}

// authProbe 量的是纯锁等待：不停地去拿那把跟增删共用的锁。它被卡多久，
// 就是真实客户握手在增删期间被卡多久。
type authProbe struct {
	stop    atomic.Bool
	done    chan struct{}
	worst   time.Duration
	blocked time.Duration
	calls   int
}

func startAuthProbe(inbound *MultiUserInbound) *authProbe {
	p := &authProbe{done: make(chan struct{})}
	go func() {
		defer close(p.done)
		ctx := context.Background()
		for !p.stop.Load() {
			t0 := time.Now()
			inbound.GetUsersCount(ctx)
			d := time.Since(t0)
			p.calls++
			p.blocked += d
			if d > p.worst {
				p.worst = d
			}
		}
	}()
	return p
}

func (p *authProbe) finish() (worst, total time.Duration, calls int) {
	p.stop.Store(true)
	<-p.done
	return p.worst, p.blocked, p.calls
}

// churnReport 跑一遍「加 churn 个、再删 churn 个」，打印实测数字。
func churnReport(tb testing.TB, label string, base, churn int, run func(*MultiUserInbound, []*protocol.MemoryUser)) time.Duration {
	tb.Helper()
	inbound := newChurnInbound(tb, base)
	fresh := make([]*protocol.MemoryUser, 0, churn)
	for i := 0; i < churn; i++ {
		fresh = append(fresh, churnUser(base+i))
	}
	probe := startAuthProbe(inbound)
	t0 := time.Now()
	run(inbound, fresh)
	elapsed := time.Since(t0)
	worst, blocked, calls := probe.finish()
	tb.Logf("%s: %d 用户底数，增删各 %d 个 → 耗时 %v；认证路径 %d 次探测累计被锁 %v，最长一次 %v",
		label, base, churn, elapsed, calls, blocked, worst)
	if n := inbound.GetUsersCount(context.Background()); n != int64(base) {
		tb.Fatalf("增删之后用户数不对：want %d, got %d", base, n)
	}
	return elapsed
}

func oneByOne(inbound *MultiUserInbound, fresh []*protocol.MemoryUser) {
	ctx := context.Background()
	for _, u := range fresh {
		if err := inbound.AddUser(ctx, u); err != nil {
			panic(err)
		}
	}
	for _, u := range fresh {
		if err := inbound.RemoveUser(ctx, u.Email); err != nil {
			panic(err)
		}
	}
}

func inBatch(inbound *MultiUserInbound, fresh []*protocol.MemoryUser) {
	ctx := context.Background()
	if err := inbound.AddUsers(ctx, fresh); err != nil {
		panic(err)
	}
	emails := make([]string, 0, len(fresh))
	for _, u := range fresh {
		emails = append(emails, u.Email)
	}
	if err := inbound.RemoveUsers(ctx, emails); err != nil {
		panic(err)
	}
}

// 完成定义：5 万用户下批量增删 5000 个客户的耗时与锁阻塞时长。
// 默认跑小一号的规模（-short 友好）；要真实数字用 -churn.base/-churn.n 调大，
// 例如 go test ./proxy/shadowsocks_2022/ -run TestUserChurn -v -timeout 30m -args -churn.base=50000 -churn.n=5000
var (
	churnBase = flag.Int("churn.base", 5000, "换手测试的用户底数")
	churnN    = flag.Int("churn.n", 500, "换手测试增删的客户数")
)

func TestUserChurnBatchBeatsOneByOne(t *testing.T) {
	base, n := *churnBase, *churnN
	one := churnReport(t, "逐个增删（改造前的唯一走法）", base, n, oneByOne)
	batch := churnReport(t, "批量增删（AddUsers/RemoveUsers）", base, n, inBatch)
	t.Logf("批量比逐个快 %.1f 倍", float64(one)/float64(batch))
	if batch*4 > one {
		t.Errorf("批量入口没有明显更快：逐个 %v vs 批量 %v —— 那 5 万实例下的月度换手就还是原样", one, batch)
	}
}

// 批量入口必须与逐个走法产生完全相同的结果，否则「快」毫无意义。
func TestBatchChurnKeepsSameUserSet(t *testing.T) {
	ctx := context.Background()
	inbound := newChurnInbound(t, 50)
	fresh := []*protocol.MemoryUser{churnUser(100), churnUser(101), churnUser(102)}
	if err := inbound.AddUsers(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if n := inbound.GetUsersCount(ctx); n != 53 {
		t.Fatalf("want 53, got %d", n)
	}
	for _, u := range fresh {
		if got := inbound.GetUser(ctx, u.Email); got == nil {
			t.Fatalf("%s 加了却查不到", u.Email)
		}
	}
	if err := inbound.RemoveUsers(ctx, []string{fresh[0].Email, fresh[2].Email}); err != nil {
		t.Fatal(err)
	}
	if n := inbound.GetUsersCount(ctx); n != 51 {
		t.Fatalf("want 51, got %d", n)
	}
	if inbound.GetUser(ctx, fresh[0].Email) != nil || inbound.GetUser(ctx, fresh[2].Email) != nil {
		t.Error("删掉的用户还在")
	}
	if inbound.GetUser(ctx, fresh[1].Email) == nil {
		t.Error("没删的用户不见了")
	}
}

// 批量增删是原子的：里面有一个坏的，整批都不生效，不留半截状态。
func TestBatchChurnRejectsDuplicatesWithoutPartialApply(t *testing.T) {
	ctx := context.Background()
	inbound := newChurnInbound(t, 10)
	dup := churnUser(3) // email 与既有用户撞车
	dup.Email = "u3@churn.test"
	if err := inbound.AddUsers(ctx, []*protocol.MemoryUser{churnUser(200), dup, churnUser(201)}); err == nil {
		t.Fatal("重复 email 必须报错")
	}
	if n := inbound.GetUsersCount(ctx); n != 10 {
		t.Fatalf("整批应回滚，want 10, got %d", n)
	}
	if inbound.GetUser(ctx, "u200@churn.test") != nil {
		t.Error("批里前面那个不该留下")
	}

	if err := inbound.RemoveUsers(ctx, []string{"u1@churn.test", "nobody@churn.test"}); err == nil {
		t.Fatal("删不存在的用户必须报错")
	}
	if n := inbound.GetUsersCount(ctx); n != 10 {
		t.Fatalf("整批应回滚，want 10, got %d", n)
	}
}
