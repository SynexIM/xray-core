package conf_test

import (
	"encoding/json"
	"github.com/xtls/xray-core/infra/conf"
	"strings"
	"testing"
)

func TestLimitConfiguration(t *testing.T) {
	var c conf.Config
	err := json.Unmarshal([]byte(`{"limit":{"node":{"capacity":"400mbps","congestion":{"enter":85,"exit":70,"holdSeconds":5}},"profiles":{"p":{"up":[{"rate":"10mbps","for":"5s"},{"rate":"4mbps","for":"20s"},{"rate":"1mbps"}],"maxSourceIps":2}},"rules":[{"ruleTag":"r","user":["a"],"profile":"p"}]}}`), &c)
	if err != nil {
		t.Fatal(err)
	}
	built, err := c.Limit.Build()
	if err != nil {
		t.Fatal(err)
	}
	if built.Profiles[0].Up.Steps[1].ForMs != 20000 || built.Profiles[0].Up.Steps[0].RateBps != 10*1024*1024 || built.Node.HoldSeconds != 5 {
		t.Fatalf("单位或外形失真: %v", built)
	}
	app, err := c.Build()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range app.App {
		found = found || a.Type == "xray.app.limit.Config"
	}
	if !found {
		t.Fatal("limit 没有进入 app 配置")
	}
}
func TestLimitRejectsInvalidConfiguration(t *testing.T) {
	for _, input := range []string{
		`{"profiles":{"p":{"up":[]}}}`,
		`{"profiles":{"p":{"up":[{"rate":"1mbps","for":"5s"},{"rate":"2mbps"}]}}}`,
		`{"profiles":{"p":{"up":[{"rate":"1mbps","for":"1s"}]}}}`,
		`{"profiles":{"p":{"up":[{"rate":"2mbps"},{"rate":"1mbps"}]}}}`,
		`{"profiles":{"p":{},"p":{}}}`,
		`{"profiles":{"p":{}},"rules":[{"ruleTag":"r","user":["a"],"profile":"missing"}]}`,
		`{"profiles":{"p":{}},"rules":[{"ruleTag":"r","profile":"p"}]}`,
		`{"profiles":{"p":{}},"rules":[{"user":["a"],"profile":"p"}]}`,
	} {
		var c conf.LimitConfig
		err := json.Unmarshal([]byte(input), &c)
		if err == nil {
			_, err = c.Build()
		}
		if err == nil {
			t.Fatalf("无效配置被接受: %s", input)
		}
	}
	var c conf.LimitConfig
	json.Unmarshal([]byte(`{"profiles":{"p":{"up":[{"rate":"5mbps","for":"20s"},{"rate":"10mbps","for":"1h"},{"rate":"1mbps"}]}}}`), &c)
	_, err := c.Build()
	if err == nil || !strings.Contains(err.Error(), "第 2 档") {
		t.Fatalf("错误没有指出档位: %v", err)
	}
}
