package shadowsocks

import (
	"strconv"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/protocol"
)

// 管理路径必须与用户数无关。目标是 5 万实例/单节点组，Del / GetByEmail 原来都是
// 线性扫 email：删一个客户要扫 5 万条，而到期释放这种事天天在发生。
//
// 认证路径（Get）故意没动：AEAD 的逐个试解是**协议缺陷不是代码缺陷**，
// 靠「SS 一律用 2022 版」规避，不在这里改。

func ssValidatorWith(tb testing.TB, n int) *Validator {
	tb.Helper()
	v := new(Validator)
	for i := 0; i < n; i++ {
		u := &protocol.MemoryUser{
			Email: "u" + strconv.Itoa(i) + "@ss.test",
			Account: &MemoryAccount{
				Cipher: &AEADCipher{KeyBytes: 16, IVBytes: 16},
				Key:    []byte("0123456789abcdef"),
			},
		}
		if err := v.Add(u); err != nil {
			tb.Fatal(err)
		}
	}
	return v
}

// 删一半用户的耗时，随底数放大 10 倍时不该跟着放大 10 倍。
func TestValidatorDeleteIsIndependentOfUserCount(t *testing.T) {
	cost := func(n int) time.Duration {
		v := ssValidatorWith(t, n)
		t0 := time.Now()
		for i := 0; i < n/2; i++ {
			if err := v.Del("u" + strconv.Itoa(i) + "@ss.test"); err != nil {
				t.Fatal(err)
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

// 索引不能跟用户表脱节：删掉的查不到，剩下的都还查得到，且换过位置的也对得上。
func TestValidatorIndexStaysConsistentAcrossDeletes(t *testing.T) {
	v := ssValidatorWith(t, 20)
	for i := 0; i < 20; i += 2 {
		if err := v.Del("u" + strconv.Itoa(i) + "@ss.test"); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		email := "u" + strconv.Itoa(i) + "@ss.test"
		got := v.GetByEmail(email)
		if i%2 == 0 && got != nil {
			t.Errorf("%s 已删却还查得到", email)
		}
		if i%2 == 1 {
			if got == nil {
				t.Errorf("%s 没删却查不到（swap-with-last 之后索引没跟上）", email)
			} else if got.Email != email {
				t.Errorf("%s 查出来的是 %s——索引指错了人", email, got.Email)
			}
		}
	}
	if got := v.GetCount(); got != 10 {
		t.Errorf("want 10, got %d", got)
	}
}

// email 撞车必须报错，不能悄悄留下两个同名用户（索引只会指向后一个，
// 前一个就成了删不掉的幽灵）。
func TestValidatorRejectsDuplicateEmail(t *testing.T) {
	v := ssValidatorWith(t, 3)
	dup := &protocol.MemoryUser{
		Email:   "u1@ss.test",
		Account: &MemoryAccount{Cipher: &AEADCipher{KeyBytes: 16, IVBytes: 16}, Key: []byte("0123456789abcdef")},
	}
	if err := v.Add(dup); err == nil {
		t.Fatal("重复 email 必须报错")
	}
	if got := v.GetCount(); got != 3 {
		t.Errorf("want 3, got %d", got)
	}
}
