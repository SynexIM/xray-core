package command

import (
	"context"
	"strconv"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/proxy"
)

// fakeManager 是一个最小的 UserManager，用来验证卸载路径的两件事：
// 批量能力被用上了，以及用户走后进程里不留僵尸运行态。
type fakeManager struct {
	users     map[string]*protocol.MemoryUser
	batchAdds int
	batchDels int
	oneAdds   int
	oneDels   int
}

func newFakeManager(emails ...string) *fakeManager {
	m := &fakeManager{users: map[string]*protocol.MemoryUser{}}
	for _, e := range emails {
		m.users[e] = &protocol.MemoryUser{Email: e, BandwidthBps: 80_000_000}
	}
	return m
}

func (m *fakeManager) AddUser(_ context.Context, u *protocol.MemoryUser) error {
	m.oneAdds++
	m.users[u.Email] = u
	return nil
}

func (m *fakeManager) RemoveUser(_ context.Context, email string) error {
	m.oneDels++
	delete(m.users, email)
	return nil
}

func (m *fakeManager) GetUser(_ context.Context, email string) *protocol.MemoryUser {
	return m.users[email]
}

func (m *fakeManager) GetUsers(context.Context) []*protocol.MemoryUser { return nil }
func (m *fakeManager) GetUsersCount(context.Context) int64             { return int64(len(m.users)) }

// batchManager 额外实现 proxy.BatchUserManager。
type batchManager struct{ *fakeManager }

func (m *batchManager) AddUsers(_ context.Context, users []*protocol.MemoryUser) error {
	m.batchAdds++
	for _, u := range users {
		m.users[u.Email] = u
	}
	return nil
}

func (m *batchManager) RemoveUsers(_ context.Context, emails []string) error {
	m.batchDels++
	for _, e := range emails {
		delete(m.users, e)
	}
	return nil
}

// 有批量能力就走批量：一批 = 一次调用，而不是 N 次。
func TestRemoveUsersPrefersBatchCapability(t *testing.T) {
	emails := []string{"a@t", "b@t", "c@t"}
	bm := &batchManager{newFakeManager(emails...)}
	if err := removeUsers(context.Background(), bm, emails); err != nil {
		t.Fatal(err)
	}
	if bm.batchDels != 1 || bm.oneDels != 0 {
		t.Errorf("应只调一次批量卸载: batch=%d one=%d", bm.batchDels, bm.oneDels)
	}
	if len(bm.users) != 0 {
		t.Errorf("用户没删干净: %v", bm.users)
	}
}

// 没有批量能力的 proxy 退回逐个走法，行为不变。
func TestRemoveUsersFallsBackToOneByOne(t *testing.T) {
	emails := []string{"a@t", "b@t"}
	fm := newFakeManager(emails...)
	if err := removeUsers(context.Background(), fm, emails); err != nil {
		t.Fatal(err)
	}
	if fm.oneDels != 2 {
		t.Errorf("want 2 次逐个卸载, got %d", fm.oneDels)
	}
}

// 用户走了，他在全局 runtimeLimiters（sync.Map，键是 *MemoryUser 指针）里的
// 限速桶必须一并清掉。5 万实例的月度换手会稳定攒出几万个这样的僵尸条目：
// 既回收不了内存，也让这个用户永远活在进程里。
func TestRemoveUsersDropsRuntimeLimiterState(t *testing.T) {
	fm := newFakeManager("ghost@t")
	u := fm.users["ghost@t"]

	first, _ := u.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	again, _ := u.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if len(first) == 0 || first[0] != again[0] {
		t.Fatal("同一个用户两次取限速桶应命中同一份缓存，测试前提不成立")
	}

	if err := removeUsers(context.Background(), fm, []string{"ghost@t"}); err != nil {
		t.Fatal(err)
	}

	after, _ := u.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if after[0] == first[0] {
		t.Error("卸载后 runtimeLimiters 里还留着这个用户的桶——僵尸条目")
	}
	u.ResetRuntimeLimiter() // 清掉这条测试自己刚重建出来的
}

// AddUsersOperation 在有批量能力时也走批量，没有就退回逐个。
func TestAddUsersOperationUsesBatchWhenAvailable(t *testing.T) {
	users := make([]*protocol.MemoryUser, 0, 3)
	for i := 0; i < 3; i++ {
		users = append(users, &protocol.MemoryUser{Email: "n" + strconv.Itoa(i) + "@t"})
	}
	bm := &batchManager{newFakeManager()}
	if err := applyAddUsers(context.Background(), bm, users); err != nil {
		t.Fatal(err)
	}
	if bm.batchAdds != 1 || bm.oneAdds != 0 {
		t.Errorf("应只调一次批量装载: batch=%d one=%d", bm.batchAdds, bm.oneAdds)
	}

	fm := newFakeManager()
	if err := applyAddUsers(context.Background(), fm, users); err != nil {
		t.Fatal(err)
	}
	if fm.oneAdds != 3 {
		t.Errorf("want 3 次逐个装载, got %d", fm.oneAdds)
	}
}

var _ proxy.UserManager = (*fakeManager)(nil)
var _ proxy.BatchUserManager = (*batchManager)(nil)
