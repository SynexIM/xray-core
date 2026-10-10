package command

import (
	"context"
	"slices"
	"testing"

	"github.com/xtls/xray-core/app/capability"
	_ "github.com/xtls/xray-core/app/policy/command"
	_ "github.com/xtls/xray-core/app/proxyman/command"
)

func TestGet(t *testing.T) {
	resp, err := (&service{}).Get(context.Background(), &GetRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Version != capability.Version() {
		t.Fatal(resp.Version)
	}
	if !slices.IsSorted(resp.Capabilities) {
		t.Fatal(resp.Capabilities)
	}
	for _, name := range []string{"policy.hotReload", "api.batch", "inbound.drain"} {
		if !slices.Contains(resp.Capabilities, name) {
			t.Fatalf("missing %s: %v", name, resp.Capabilities)
		}
	}
}
