package api

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	limitcommand "github.com/xtls/xray-core/app/limit/command"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestLimitCLIArgumentShapes(t *testing.T) {
	dir := t.TempDir()
	profilePath := filepath.Join(dir, "profiles.json")
	if err := os.WriteFile(profilePath, []byte(`{"limit":{"profiles":{"p":{"up":[{"rate":"5000000bps"}]}}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	data, err := readLimitArgument([]string{profilePath}, false)
	if err != nil {
		t.Fatal(err)
	}
	config, _, err := decodeLimitConfig(data)
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := buildProfileArgument(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 1 || profiles[0].Up.Steps[0].RateBps != 5_000_000 {
		t.Fatal("配置外形或单位失真")
	}
	pagePath := filepath.Join(dir, "page.json")
	if err := os.WriteFile(pagePath, []byte(`{"offset":1,"limit":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	data, err = readLimitArgument([]string{pagePath}, false)
	if err != nil {
		t.Fatal(err)
	}
	request := &limitcommand.ListRulesRequest{}
	if err = protojson.Unmarshal(data, request); err != nil {
		t.Fatal(err)
	}
	if request.Offset != 1 || request.Limit != 2 {
		t.Fatal("分页 limit 被当成配置 envelope")
	}
	data, err = readLimitArgument(nil, true)
	if err != nil {
		t.Fatal(err)
	}
	var empty map[string]any
	if err = json.Unmarshal(data, &empty); err != nil || len(empty) != 0 {
		t.Fatal("只读命令没有空查询")
	}
	if _, err = readLimitArgument(nil, false); err == nil {
		t.Fatal("修改命令允许缺少文件")
	}
	if _, _, err = decodeLimitConfig([]byte(`{"profilez":{}}`)); err == nil {
		t.Fatal("拼错修改参数被静默接受")
	}
}
