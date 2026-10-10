package api

import (
	capabilityservice "github.com/xtls/xray-core/app/capability/command"
	"github.com/xtls/xray-core/main/commands/base"
)

var cmdCapability = &base.Command{
	CustomFlags: true,
	UsageLine:   "{{.Exec}} api capability [--server=unix:///run/xray/api.sock]",
	Short:       "Read version and registered capabilities",
	Long:        "Return the upstream/fork version and sorted capabilities declared by linked modules.",
	Run:         executeCapability,
}

func executeCapability(cmd *base.Command, args []string) {
	setSharedFlags(cmd)
	cmd.Flag.Parse(args)
	if cmd.Flag.NArg() != 0 {
		base.Fatalf("capability takes no positional arguments")
	}
	conn, ctx, close := dialAPIServer()
	defer close()
	resp, err := capabilityservice.NewCapabilityServiceClient(conn).Get(ctx, &capabilityservice.GetRequest{})
	if err != nil {
		base.Fatalf("failed to get capabilities: %s", err)
	}
	showJSONResponse(resp)
}
