package http_test

import (
	"testing"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/proxy/http"
	"github.com/xtls/xray-core/proxy/socks"
)

func TestUserStoreBootAccountsCarryLimits(t *testing.T) {
	store := http.NewUserStore(2, nil, []*http.UserAccount{
		{Username: "alice", Password: "pw1", BandwidthBps: 1000, ConnLimit: 3,
			CommittedBps: 400, CommittedBurstBytes: 5000},
		{Username: "bob", Password: "pw2"},
	})

	u, ok := store.Authenticate("alice", "pw1")
	if !ok {
		t.Fatal("alice should authenticate")
	}
	if u.BandwidthBps != 1000 || u.ConnLimit != 3 {
		t.Fatalf("alice limits = %d/%d, want 1000/3", u.BandwidthBps, u.ConnLimit)
	}
	if u.Level != 2 {
		t.Fatalf("alice level = %d, want 2", u.Level)
	}
	// 双速率也要一路走到 MemoryUser：静态入站这条路不经过 protocol.User，
	// 只在这里把配置翻成用户，漏掉就是静默不生效。
	if u.CommittedBps != 400 || u.CommittedBurstBytes != 5000 {
		t.Fatalf("alice 双速率 = %d/%d, want 400/5000", u.CommittedBps, u.CommittedBurstBytes)
	}

	b, ok := store.Authenticate("bob", "pw2")
	if !ok {
		t.Fatal("bob should authenticate")
	}
	if b.CommittedBps != 0 || b.CommittedBurstBytes != 0 {
		t.Fatalf("bob 没配双速率却冒出值：%d/%d", b.CommittedBps, b.CommittedBurstBytes)
	}

	// Same pointer every time so fair-share / conn counting stays per-user.
	u2, _ := store.Authenticate("alice", "pw1")
	if u != u2 {
		t.Fatal("Authenticate must return the same *MemoryUser instance per username")
	}

	if _, ok := store.Authenticate("alice", "wrong"); ok {
		t.Fatal("wrong password must fail")
	}
	if _, ok := store.Authenticate("ghost", "pw"); ok {
		t.Fatal("unknown user must fail")
	}
}

func TestUserStoreEmpty(t *testing.T) {
	if !http.NewUserStore[*http.UserAccount](0, nil, nil).Empty() {
		t.Fatal("store with no users should be Empty")
	}
	if http.NewUserStore[*http.UserAccount](0, map[string]string{"a": "b"}, nil).Empty() {
		t.Fatal("store with a legacy account should not be Empty")
	}
}

// TestUserStoreAddRemoveRoundTrip mirrors the runtime AddUser -> RemoveUser ->
// AddUser path (Xray has no UpdateUser). After removal the same email must
// re-authenticate against the new instance, not the stale one.
func TestUserStoreAddRemoveRoundTrip(t *testing.T) {
	store := http.NewUserStore[*http.UserAccount](0, nil, nil)

	add := func(bps uint64) *protocol.MemoryUser {
		u := &protocol.MemoryUser{Email: "carol", BandwidthBps: bps}
		if err := store.Add(u); err != nil {
			t.Fatalf("Add: %v", err)
		}
		return u
	}

	first := add(500)
	got, ok := store.Authenticate("carol", "")
	if !ok || got != first {
		t.Fatal("after Add, authenticate should return the added user")
	}

	store.Remove("carol")
	if _, ok := store.Authenticate("carol", ""); ok {
		t.Fatal("after Remove, user must not authenticate")
	}

	second := add(900)
	got, ok = store.Authenticate("carol", "")
	if !ok || got != second || got == first {
		t.Fatal("re-added user must be the new instance with new limits")
	}
	if got.BandwidthBps != 900 {
		t.Fatalf("re-added limits = %d, want 900", got.BandwidthBps)
	}
}

func TestUserStoreRuntimeAddUsesAccountUsernameForAuthAndEmailForLookup(t *testing.T) {
	store := http.NewUserStore[*http.UserAccount](0, nil, nil)
	account, err := (&socks.Account{Username: "proxy-user", Password: "proxy-pass"}).AsAccount()
	if err != nil {
		t.Fatalf("AsAccount: %v", err)
	}
	user := &protocol.MemoryUser{
		Email:        "sr_instance_email",
		Account:      account,
		BandwidthBps: 1000,
		ConnLimit:    2,
	}

	if err := store.Add(user); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, ok := store.Authenticate("proxy-user", "proxy-pass")
	if !ok || got != user {
		t.Fatal("runtime AddUser must authenticate by account username/password")
	}
	if _, ok := store.Authenticate("sr_instance_email", "proxy-pass"); ok {
		t.Fatal("runtime AddUser email is the route key, not the auth username")
	}
	if store.Get("sr_instance_email") != user {
		t.Fatal("Get must still resolve by email for xray control-plane lookups")
	}

	store.Remove("sr_instance_email")
	if _, ok := store.Authenticate("proxy-user", "proxy-pass"); ok {
		t.Fatal("Remove by email must delete the username auth entry")
	}
}

// TestRuntimeConnectionCap verifies the protocol-agnostic per-user connection
// cap enforced by the dispatcher keys off the *MemoryUser instance.
func TestRuntimeConnectionCap(t *testing.T) {
	u := &protocol.MemoryUser{Email: "dave", ConnLimit: 2}

	r1, ok := u.AcquireRuntimeConnection()
	if !ok {
		t.Fatal("1st connection should be allowed")
	}
	r2, ok := u.AcquireRuntimeConnection()
	if !ok {
		t.Fatal("2nd connection should be allowed")
	}
	if _, ok := u.AcquireRuntimeConnection(); ok {
		t.Fatal("3rd connection must be rejected (over cap)")
	}

	r1()
	r3, ok := u.AcquireRuntimeConnection()
	if !ok {
		t.Fatal("after releasing one, a new connection should be allowed")
	}
	r2()
	r3()

	// Unlimited user: never rejected.
	free := &protocol.MemoryUser{Email: "free"}
	for i := 0; i < 100; i++ {
		if _, ok := free.AcquireRuntimeConnection(); !ok {
			t.Fatal("zero ConnLimit means unlimited")
		}
	}
	free.ResetRuntimeConnections()
}
