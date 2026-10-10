package conf

import (
	"os"
	"strings"
	"testing"

	confjson "github.com/xtls/xray-core/infra/conf/json"
	"io"
)

func TestDecodeStrictRejectsUnknown(t *testing.T) {
	for _, tc := range []struct{ input, path string }{
		{`{"inbonds":[]}`, "$.inbonds"},
		{`{"inbounds":[{"protocol":"socks","port":1080,"settings":{"acounts":[]}}]}`, "$.inbounds[0].settings.acounts"},
		{`{"inbounds":[{"protocol":"vless","port":1080,"settings":{"clients":[{"id":"x","flwo":"x"}]}}]}`, ".clients[0].flwo"},
		{`{"outbounds":[{"protocol":"vmess","settings":{"vnext":[{"users":[{"idd":"x"}]}]}}]}`, ".users[0].idd"},
		{`{"limit":{"profiles":{"p":{"dowm":[]}}}}`, "$.limit.profiles.p.dowm"},
		{`{"routing":{"rules":[{"outboundTag":"a","uesr":[]}]}}`, "$.routing.rules[0].uesr"},
		{`{} {}`, "after top-level value"},
	} {
		_, err := DecodeStrict([]byte(tc.input))
		if err == nil || !strings.Contains(err.Error(), tc.path) {
			t.Fatalf("path=%s err=%v", tc.path, err)
		}
	}
}

func TestDecodeStrictExamples(t *testing.T) {
	for _, name := range []string{"node-line", "node-ip"} {
		f, err := os.Open("../../deploy/examples/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(&confjson.Reader{Reader: f})
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = DecodeStrict(data); err != nil {
			t.Fatal(err)
		}
	}
	c, err := DecodeStrict([]byte(`{"inbounds":[{"protocol":"socks","listen":"127.0.0.1","port":1080,"settings":{"auth":"noauth"}}],"outbounds":[{"protocol":"freedom"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Build(); err != nil {
		t.Fatal(err)
	}
}
