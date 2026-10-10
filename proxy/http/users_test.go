package http_test

import (
	"testing"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/proxy/http"
	"github.com/xtls/xray-core/proxy/socks"
)

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

	add := func(_ uint64) *protocol.MemoryUser {
		account, err := (&http.Account{Username: "carol-login", Password: "test-only"}).AsAccount()
		if err != nil {
			t.Fatal(err)
		}
		u := &protocol.MemoryUser{Email: "carol", Account: account}
		if err := store.Add(u); err != nil {
			t.Fatalf("Add: %v", err)
		}
		return u
	}

	first := add(500)
	got, ok := store.Authenticate("carol-login", "test-only")
	if !ok || got != first {
		t.Fatal("after Add, authenticate should return the added user")
	}

	store.Remove("carol")
	if _, ok := store.Authenticate("carol-login", "test-only"); ok {
		t.Fatal("after Remove, user must not authenticate")
	}

	second := add(900)
	got, ok = store.Authenticate("carol-login", "test-only")
	if !ok || got != second || got == first {
		t.Fatal("re-added user must be the new instance with new identity")
	}

}

func TestBootUsernameDoesNotInventEmail(t *testing.T) {
	store := http.NewUserStore[*http.UserAccount](2, map[string]string{"login": "test-only"}, nil)
	identity, ok := store.Authenticate("login", "test-only")
	if !ok || identity.Email != "" {
		t.Fatalf("认证名被伪造成 email: %+v", identity)
	}
}

func TestRuntimeIdentityDoesNotBecomeCredential(t *testing.T) {
	store := http.NewUserStore[*http.UserAccount](0, nil, nil)
	if err := store.Add(&protocol.MemoryUser{Email: "identity"}); err == nil {
		t.Fatal("缺少用户名却回退到 email")
	}
}

func TestUserStoreRuntimeAddUsesAccountUsernameForAuthAndEmailForLookup(t *testing.T) {
	store := http.NewUserStore[*http.UserAccount](0, nil, nil)
	account, err := (&socks.Account{Username: "proxy-user", Password: "proxy-pass"}).AsAccount()
	if err != nil {
		t.Fatalf("AsAccount: %v", err)
	}
	user := &protocol.MemoryUser{
		Email:   "sr_instance_email",
		Account: account,
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
