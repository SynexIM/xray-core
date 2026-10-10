package api

import (
	"fmt"

	handlerService "github.com/xtls/xray-core/app/proxyman/command"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/main/commands/base"
)

var cmdSetOutboundRateLimit = &base.Command{
	CustomFlags: true,
	UsageLine:   "{{.Exec}} api outboundratelimit [--server=127.0.0.1:8080] --tag=<outbound-tag> --bit-per-sec=<rate>",
	Short:       "Hot-update one outbound's aggregate rate limit",
	Long: `
Hot-update the aggregate payload rate shared by all connections using one outbound.
The outbound must have been created with rateLimitBitPerSec present. A rate of zero
disables the cap while keeping established connections ready for a later hot enable.

Arguments:

	-s, -server <server:port>
		The API server address. Default 127.0.0.1:8080

	-t, -timeout <seconds>
		Timeout in seconds for calling API. Default 3

	-tag <outbound-tag>
		The exact outbound tag to update.

	-bit-per-sec <rate>
		Aggregate payload rate in bits per second. Zero disables the cap.

Example:

	{{.Exec}} {{.LongName}} --server=127.0.0.1:8080 --tag=shared-egress --bit-per-sec=80000000
`,
	Run: executeSetOutboundRateLimit,
}

func executeSetOutboundRateLimit(cmd *base.Command, args []string) {
	setSharedFlags(cmd)
	var tag string
	var bitPerSec uint64
	cmd.Flag.StringVar(&tag, "tag", "", "outbound tag")
	cmd.Flag.Uint64Var(&bitPerSec, "bit-per-sec", 0, "aggregate payload rate in bits per second")
	cmd.Flag.Parse(args)
	if tag == "" {
		base.Fatalf("outbound tag is required")
	}
	if cmd.Flag.NArg() != 0 {
		base.Fatalf("unexpected positional arguments: %v", cmd.Flag.Args())
	}

	conn, ctx, close := dialAPIServer()
	defer close()

	client := handlerService.NewHandlerServiceClient(conn)
	response, err := client.AlterOutbound(ctx, newOutboundRateLimitRequest(tag, bitPerSec))
	if err != nil {
		base.Fatalf("failed to set outbound rate limit: %s", err)
	}
	fmt.Printf("updated %s to %d bit/s\n", tag, bitPerSec)
	showJSONResponse(response)
}

func newOutboundRateLimitRequest(tag string, bitPerSec uint64) *handlerService.AlterOutboundRequest {
	return &handlerService.AlterOutboundRequest{
		Tag: tag,
		Operation: serial.ToTypedMessage(&handlerService.SetOutboundRateLimitOperation{
			RateLimitBitPerSec: bitPerSec,
		}),
	}
}
