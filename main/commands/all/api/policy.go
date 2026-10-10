package api

import (
	"encoding/json"
	"io"

	policyservice "github.com/xtls/xray-core/app/policy/command"
	"github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/main/commands/base"
)

var cmdPolicy = &base.Command{
	UsageLine: "{{.Exec}} api policy",
	Short:     "Read or replace policy levels",
	Long:      "Policy updates affect new connections. Existing connections retain their policy.",
	Commands:  []*base.Command{cmdPolicySet, cmdPolicyGet},
}

var cmdPolicySet = &base.Command{
	CustomFlags: true,
	UsageLine:   "{{.Exec}} api policy set [--server=unix:///run/xray/api.sock] policy.json",
	Short:       "Replace all policy levels",
	Long:        `Read a policy object with a levels map (connIdle, handshake, uplinkOnly, downlinkOnly, statsUserUplink, statsUserDownlink, bufferSize). Omitted levels are removed. System policy cannot be changed.`,
	Run:         executePolicySet,
}

var cmdPolicyGet = &base.Command{
	CustomFlags: true,
	UsageLine:   "{{.Exec}} api policy get [--server=unix:///run/xray/api.sock]",
	Short:       "Read effective policy levels",
	Run:         executePolicyGet,
}

func executePolicySet(cmd *base.Command, args []string) {
	setSharedFlags(cmd)
	cmd.Flag.Parse(args)
	if cmd.Flag.NArg() != 1 {
		base.Fatalf("provide one policy JSON file (or stdin:)")
	}
	r, err := loadArg(cmd.Flag.Arg(0))
	if err != nil {
		base.Fatalf("failed to read policy: %s", err)
	}
	var config conf.PolicyConfig
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		base.Fatalf("invalid policy: %s", err)
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		base.Fatalf("policy must contain one JSON object")
	}
	if config.System != nil {
		base.Fatalf("system policy cannot be changed")
	}
	p, err := config.Build()
	if err != nil {
		base.Fatalf("invalid policy: %s", err)
	}
	conn, ctx, close := dialAPIServer()
	defer close()
	resp, err := policyservice.NewPolicyServiceClient(conn).SetLevels(ctx, &policyservice.SetLevelsRequest{Levels: p.Level})
	if err != nil {
		base.Fatalf("failed to set policy levels: %s", err)
	}
	showJSONResponse(resp)
}

func executePolicyGet(cmd *base.Command, args []string) {
	setSharedFlags(cmd)
	cmd.Flag.Parse(args)
	if cmd.Flag.NArg() != 0 {
		base.Fatalf("policy get takes no positional arguments")
	}
	conn, ctx, close := dialAPIServer()
	defer close()
	resp, err := policyservice.NewPolicyServiceClient(conn).GetLevels(ctx, &policyservice.GetLevelsRequest{})
	if err != nil {
		base.Fatalf("failed to get policy levels: %s", err)
	}
	showJSONResponse(resp)
}
