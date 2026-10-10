package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/xtls/xray-core/app/limit"
	limitcommand "github.com/xtls/xray-core/app/limit/command"
	handlercommand "github.com/xtls/xray-core/app/proxyman/command"
	"github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/main/commands/base"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var cmdLimit = &base.Command{
	UsageLine: "{{.Exec}} api limit",
	Short:     "管理节点限速",
	Long: `经 LimitService 管理节点容量、限速档和规则。速率使用带单位字符串，时长使用 20s、2h。

文件可以是 limit 段本身，也可以是包含 limit 的 Xray 配置。
setnode: {"node":{"capacity":"400mbps","congestion":{"enter":85,"exit":70,"holdSeconds":5}}}
putprofiles: {"profiles":{"p":{"up":[{"rate":"5mbps"}]}}}
addrules: {"rules":[{"ruleTag":"r","user":["a"],"profile":"p"}]}
rmprofiles: {"names":["p"]}
rmrules: {"ruleTags":["r"]}
lsrules: {"offset":0,"limit":100}
status: {"ruleTag":"r"}；不传文件只查节点状态。

例：{{.Exec}} api limit putprofiles -s unix:/run/xray/api.sock profiles.json
`,
}
var cmdKillConnections = &base.Command{
	CustomFlags: true,
	UsageLine:   "{{.Exec}} api killconn [--server=unix:/run/xray/api.sock] <emails.json>",
	Short:       "按 email 断开现有连接，保留用户",
	Long:        `文件内容为 {"emails":["a","b"]}。断开后用户仍可认证，重连会重新选择路由。`,
	Run:         executeKillConnections,
}

func init() {
	for _, verb := range []struct{ name, help string }{
		{"setnode", "设置节点容量和拥挤阈值"},
		{"putprofiles", "新增或原地更新限速档"},
		{"rmprofiles", "删除未被规则引用的限速档"},
		{"addrules", "批量新增或替换限速规则"},
		{"rmrules", "批量删除限速规则"},
		{"lsrules", "分页查询规则标识和限速档"},
		{"status", "查询节点或指定规则的运行状态"},
	} {
		cmdLimit.Commands = append(cmdLimit.Commands, &base.Command{
			CustomFlags: true, UsageLine: "{{.Exec}} api limit " + verb.name + " [--server=unix:/run/xray/api.sock] [request.json]",
			Short: verb.help, Long: cmdLimit.Long, Run: executeLimit,
		})
	}
}
func readLimitArgument(args []string, optional bool) ([]byte, error) {
	if len(args) == 0 && optional {
		return []byte("{}"), nil
	}
	if len(args) != 1 {
		return nil, fmt.Errorf("需要一个 JSON 文件参数，或 stdin:")
	}
	reader, err := loadArg(args[0])
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	var envelope map[string]json.RawMessage
	if err = json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	// lsrules 的 limit 是数字，只把对象形的 limit 当作配置段。
	if value, ok := envelope["limit"]; ok && len(bytes.TrimSpace(value)) > 0 && bytes.TrimSpace(value)[0] == '{' {
		data = value
	}
	return data, nil
}
func decodeLimitConfig(data []byte) (*conf.LimitConfig, map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, nil, err
	}
	var config conf.LimitConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, nil, err
	}
	return &config, fields, nil
}
func buildProfileArgument(config *conf.LimitConfig) ([]*limit.Profile, error) {
	names := make([]string, 0, len(config.Profiles))
	for name := range config.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	profiles := make([]*limit.Profile, 0, len(names))
	for _, name := range names {
		profile, err := config.Profiles[name].Build(name)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, profile)
	}
	return profiles, nil
}
func executeLimit(cmd *base.Command, args []string) {
	setSharedFlags(cmd)
	cmd.Flag.Parse(args)
	verb := cmd.Name()
	data, err := readLimitArgument(cmd.Flag.Args(), verb == "lsrules" || verb == "status")
	if err != nil {
		base.Fatalf("读取限速参数失败: %s", err)
	}
	conn, ctx, close := dialAPIServer()
	defer close()
	client := limitcommand.NewLimitServiceClient(conn)
	var response proto.Message
	switch verb {
	case "setnode", "putprofiles", "addrules":
		config, fields, decodeErr := decodeLimitConfig(data)
		if decodeErr != nil {
			base.Fatalf("解析限速配置失败: %s", decodeErr)
		}
		switch verb {
		case "setnode":
			if _, ok := fields["node"]; !ok {
				base.Fatalf("文件缺少 node 字段")
			}
			node, buildErr := config.Node.Build()
			if buildErr != nil {
				base.Fatalf("构建 node 失败: %s", buildErr)
			}
			response, err = client.SetNode(ctx, &limitcommand.SetNodeRequest{Node: node})
		case "putprofiles":
			if _, ok := fields["profiles"]; !ok {
				base.Fatalf("文件缺少 profiles 字段")
			}
			profiles, buildErr := buildProfileArgument(config)
			if buildErr != nil {
				base.Fatalf("构建 profiles 失败: %s", buildErr)
			}
			response, err = client.PutProfiles(ctx, &limitcommand.PutProfilesRequest{Profiles: profiles})
		case "addrules":
			if _, ok := fields["rules"]; !ok {
				base.Fatalf("文件缺少 rules 字段")
			}
			var rules []*limit.Rule
			for _, rule := range config.Rules {
				if rule == nil {
					base.Fatalf("rules 不能含 null")
				}
				native := &limit.Rule{RuleTag: rule.RuleTag, Profile: rule.Profile, User: rule.User, InboundTag: rule.InboundTag, OutboundTag: rule.OutboundTag}
				if validateErr := limit.ValidateRule(native); validateErr != nil {
					base.Fatalf("构建 rule 失败: %s", validateErr)
				}
				rules = append(rules, native)
			}
			response, err = client.AddRules(ctx, &limitcommand.AddRulesRequest{Rules: rules})
		}
	case "rmprofiles":
		request := &limitcommand.RemoveProfilesRequest{}
		if err = protojson.Unmarshal(data, request); err == nil {
			response, err = client.RemoveProfiles(ctx, request)
		}
	case "rmrules":
		request := &limitcommand.RemoveRulesRequest{}
		if err = protojson.Unmarshal(data, request); err == nil {
			response, err = client.RemoveRules(ctx, request)
		}
	case "lsrules":
		request := &limitcommand.ListRulesRequest{}
		if err = protojson.Unmarshal(data, request); err == nil {
			response, err = client.ListRules(ctx, request)
		}
	case "status":
		request := &limitcommand.GetStatusRequest{}
		if err = protojson.Unmarshal(data, request); err == nil {
			response, err = client.GetStatus(ctx, request)
		}
	}
	if err != nil {
		base.Fatalf("limit %s 失败: %s", verb, err)
	}
	showJSONResponse(response)
}
func executeKillConnections(cmd *base.Command, args []string) {
	setSharedFlags(cmd)
	cmd.Flag.Parse(args)
	data, err := readLimitArgument(cmd.Flag.Args(), false)
	if err != nil {
		base.Fatalf("读取 emails 失败: %s", err)
	}
	request := &handlercommand.KillUserConnectionsRequest{}
	if err = protojson.Unmarshal(data, request); err != nil {
		base.Fatalf("解析 emails 失败: %s", err)
	}
	conn, ctx, close := dialAPIServer()
	defer close()
	response, err := handlercommand.NewHandlerServiceClient(conn).KillUserConnections(ctx, request)
	if err != nil {
		base.Fatalf("killconn 失败: %s", err)
	}
	showJSONResponse(response)
}
