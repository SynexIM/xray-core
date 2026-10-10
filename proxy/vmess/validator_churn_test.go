package vmess

import (
	"strconv"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/uuid"
)

// 同 shadowsocks：管理路径必须与用户数无关。Remove 原来线性扫 email，
// 5 万实例/单节点组下删一个客户要扫 5 万条。
// 认证路径（GetAEAD）本来就是 O(1) 哈希查表，没动。

func vmessValidatorWith(tb testing.TB, n int) *TimedUserValidator {
	tb.Helper()
	v := NewTimedUserValidator()
	for i := 0; i < n; i++ {
		id := uuid.New()
		u := &protocol.MemoryUser{
			Email:   "u" + strconv.Itoa(i) + "@vmess.test",
			Account: &MemoryAccount{ID: protocol.NewID(id)},
		}
		if err := v.Add(u); err != nil {
			tb.Fatal(err)
		}
	}
	return v
}

func TestRemoveIsIndependentOfUserCount(t *testing.T) {
	cost := func(n int) time.Duration {
		v := vmessValidatorWith(t, n)
		t0 := time.Now()
		for i := 0; i < n/2; i++ {
			if !v.Remove("u" + strconv.Itoa(i) + "@vmess.test") {
				t.Fatalf("u%d 删不掉", i)
			}
		}
		d := time.Since(t0)
		if got := v.GetCount(); got != int64(n-n/2) {
			t.Fatalf("删完剩 %d，want %d", got, n-n/2)
		}
		return d
	}
	small, large := cost(2_000), cost(20_000)
	perOpSmall := small / 1_000
	perOpLarge := large / 10_000
	t.Logf("2,000 用户底数删 1,000 个: %v（每次 %v）；20,000 用户底数删 10,000 个: %v（每次 %v）",
		small, perOpSmall, large, perOpLarge)
	if perOpLarge > perOpSmall*5 {
		t.Errorf("底数放大 10 倍后单次删除慢了 %.1f 倍——管理路径还是 O(N)",
			float64(perOpLarge)/float64(perOpSmall))
	}
}

// 索引与用户表不能脱节；AEAD 解码器表也要跟着一起清（不清的话删掉的用户
// 还能继续握手认证成功——那是个安全问题，不只是内存问题）。
func TestRemoveKeepsIndexAndAEADTableConsistent(t *testing.T) {
	v := vmessValidatorWith(t, 20)
	removed := v.GetUsers()[3]
	var cmdkey [16]byte
	copy(cmdkey[:], removed.Account.(*MemoryAccount).ID.CmdKey())

	if !v.Remove(removed.Email) {
		t.Fatal("删不掉")
	}
	if v.Remove(removed.Email) {
		t.Error("同一个 email 删了两次都成功——索引没清")
	}
	if got := v.GetCount(); got != 19 {
		t.Fatalf("want 19, got %d", got)
	}
	for _, u := range v.GetUsers() {
		if u.Email == removed.Email {
			t.Fatal("删掉的用户还在表里")
		}
	}
	// 剩下的每个人都还能按 email 找回自己（swap-with-last 之后索引要跟上）。
	for _, u := range v.GetUsers() {
		if !v.Remove(u.Email) {
			t.Fatalf("%s 应该还能删掉，说明索引指错了", u.Email)
		}
	}
	if got := v.GetCount(); got != 0 {
		t.Errorf("want 0, got %d", got)
	}
}
