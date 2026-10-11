package socks

import (
	"context"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
)

// Client is a Socks5 client.
type Client struct {
	server        *protocol.ServerSpec
	policyManager policy.Manager
	caps          *upstreamCaps
	warm          *warmPool
}

// NewClient create a new Socks5 client based on the given config.
func NewClient(ctx context.Context, config *ClientConfig) (*Client, error) {
	if config.Server == nil {
		return nil, errors.New(`no target server found`)
	}
	server, err := protocol.NewServerSpecFromPB(config.Server)
	if err != nil {
		return nil, errors.New("failed to get server spec").Base(err)
	}

	v := core.MustFromContext(ctx)
	c := &Client{
		server:        server,
		policyManager: v.GetFeature(policy.ManagerType()).(policy.Manager),
		caps:          &upstreamCaps{noStagger: config.NoPipeline, noEarly: config.NoEarlyData},
		warm:          &warmPool{disabled: config.NoPrewarm},
	}

	return c, nil
}

// Process implements proxy.Outbound.Process.
func (c *Client) Process(ctx context.Context, link *transport.Link, dialer internet.Dialer) error {
	outbounds := session.OutboundsFromContext(ctx)
	ob := outbounds[len(outbounds)-1]
	if !ob.Target.IsValid() {
		return errors.New("target not specified.")
	}
	ob.Name = "socks"
	ob.CanSpliceCopy = 2
	// Destination of the inner request.
	destination := ob.Target

	// Outbound server.
	server := c.server
	dest := server.Destination

	p := c.policyManager.ForLevel(0)

	request := &protocol.RequestHeader{
		Version: socks5Version,
		Command: protocol.RequestCommandTCP,
		Address: destination.Address,
		Port:    destination.Port,
	}

	if destination.Network == net.Network_UDP {
		request.Command = protocol.RequestCommandUDP
	}

	user := server.User
	if user != nil {
		request.User = user
		p = c.policyManager.ForLevel(user.Level)
	}

	// First payload rides with the request when the upstream passed the self-check (fork: upstream.go).
	var early []byte
	if _, ok := c.caps.get(); ok && request.Command == protocol.RequestCommandTCP {
		var err error
		if early, err = takeFirstPayload(link.Reader); err != nil {
			return errors.New("failed to read first payload").Base(err)
		}
	}

	// Connection to the outbound server, with the request already answered (fork: warm.go).
	conn, udpRequest, err := c.connect(ctx, dialer, request, early, p)
	if err != nil {
		return errors.New("failed to establish connection to server").Base(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			errors.LogInfoInner(ctx, err, "failed to closed connection")
		}
	}()
	if udpRequest != nil {
		if udpRequest.Address == net.AnyIP || udpRequest.Address == net.AnyIPv6 {
			udpRequest.Address = dest.Address
		}
	}

	if err := conn.SetDeadline(time.Time{}); err != nil {
		errors.LogInfoInner(ctx, err, "failed to clear deadline after handshake")
	}

	var newCtx context.Context
	var newCancel context.CancelFunc
	if session.TimeoutOnlyFromContext(ctx) {
		newCtx, newCancel = context.WithCancel(context.Background())
	}

	ctx, cancel := context.WithCancel(ctx)
	timer := signal.CancelAfterInactivity(ctx, func() {
		cancel()
		if newCancel != nil {
			newCancel()
		}
	}, p.Timeouts.ConnectionIdle)

	var requestFunc func() error
	var responseFunc func() error
	if request.Command == protocol.RequestCommandTCP {
		requestFunc = func() error {
			defer timer.SetTimeout(p.Timeouts.DownlinkOnly)
			return buf.Copy(link.Reader, buf.NewWriter(conn), buf.UpdateActivity(timer))
		}
		responseFunc = func() error {
			ob.CanSpliceCopy = 1
			defer timer.SetTimeout(p.Timeouts.UplinkOnly)
			return buf.Copy(buf.NewReader(conn), link.Writer, buf.UpdateActivity(timer))
		}
	} else if request.Command == protocol.RequestCommandUDP {
		udpConn, err := dialer.Dial(ctx, udpRequest.Destination())
		if err != nil {
			return errors.New("failed to create UDP connection").Base(err)
		}
		defer udpConn.Close()
		requestFunc = func() error {
			defer timer.SetTimeout(p.Timeouts.DownlinkOnly)
			writer := &UDPWriter{Writer: udpConn, Request: request}
			return buf.Copy(link.Reader, writer, buf.UpdateActivity(timer))
		}
		responseFunc = func() error {
			ob.CanSpliceCopy = 1
			defer timer.SetTimeout(p.Timeouts.UplinkOnly)
			reader := &UDPReader{Reader: udpConn}
			return buf.Copy(reader, link.Writer, buf.UpdateActivity(timer))
		}
	}

	if newCtx != nil {
		ctx = newCtx
	}

	responseDonePost := task.OnSuccess(responseFunc, task.Close(link.Writer))
	if err := task.Run(ctx, requestFunc, responseDonePost); err != nil {
		return errors.New("connection ends").Base(err)
	}

	return nil
}

func init() {
	common.Must(common.RegisterConfig((*ClientConfig)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return NewClient(ctx, config.(*ClientConfig))
	}))
}
