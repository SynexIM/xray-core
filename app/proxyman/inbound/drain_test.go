package inbound

import (
	"context"
	"io"
	stdnet "net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/mux"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// A protocol echo fixture exercises real listening sockets and live streams.
// Protocol-specific authentication remains covered by the separate core suite.
type echoInbound struct {
	closed atomic.Int32
	cancel context.CancelFunc
}

func (*echoInbound) Network() []xnet.Network {
	return []xnet.Network{xnet.Network_TCP, xnet.Network_UDP, xnet.Network_UNIX}
}
func (p *echoInbound) Close() error { p.closed.Add(1); p.cancel(); return nil }
func (*echoInbound) Process(ctx context.Context, _ xnet.Network, conn stat.Connection, _ routing.Dispatcher) error {
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-finished:
		}
	}()
	return buf.Copy(buf.NewReader(conn), buf.NewWriter(conn))
}

func echo(t *testing.T, conn stdnet.Conn, message string) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte(message)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(message))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != message {
		t.Fatal("existing stream changed")
	}
}

func tcpFixture(t *testing.T) (*Manager, *AlwaysOnInboundHandler, *tcpWorker, *echoInbound) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := &echoInbound{cancel: cancel}
	stream, err := internet.ToMemoryStreamConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	w := &tcpWorker{address: xnet.LocalHostIP, proxy: p, stream: stream, ctx: ctx}
	reservation, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	w.port = xnet.Port(reservation.Addr().(*stdnet.TCPAddr).Port)
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	h := &AlwaysOnInboundHandler{tag: "test-inbound", proxy: p, workers: []worker{w}, mux: &mux.Server{}}
	m, err := New(ctx, &proxyman.InboundConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	if err := m.AddHandler(ctx, h); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, h, w, p
}

func TestDrainPreservesTCPStreamAndResumeAfterPortConflict(t *testing.T) {
	m, h, w, p := tcpFixture(t)
	address := w.hub.Addr().String()
	conn, err := stdnet.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	echo(t, conn, "before")
	for i := 0; i < 2; i++ {
		if err := m.DrainHandler(context.Background(), h.tag); err != nil {
			t.Fatal(err)
		}
	}
	if p.closed.Load() != 0 {
		t.Fatal("drain closed proxy/user state")
	}
	if incoming, err := stdnet.DialTimeout("tcp", address, 150*time.Millisecond); err == nil {
		incoming.Close()
		t.Fatal("new TCP connection accepted after drain")
	}
	echo(t, conn, "while-drained")
	blocker, err := stdnet.Listen("tcp", address)
	if err != nil {
		t.Fatal("drain did not release listening port")
	}
	if err := m.ResumeHandler(context.Background(), h.tag); err == nil {
		blocker.Close()
		t.Fatal("resume ignored occupied port")
	}
	echo(t, conn, "after-failed-resume")
	if err := blocker.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := m.ResumeHandler(context.Background(), h.tag); err != nil {
			t.Fatal(err)
		}
	}
	newConn, err := stdnet.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer newConn.Close()
	echo(t, newConn, "new-after-resume")
	echo(t, conn, "old-after-resume")
	if p.closed.Load() != 0 {
		t.Fatal("resume rebuilt proxy state")
	}
}

func TestDrainPreflightsAllTransports(t *testing.T) {
	m, h, w, p := tcpFixture(t)
	address := w.hub.Addr().String()
	// gRPC server Close would terminate existing streams; the current worker
	// implementation must reject it before draining another worker's socket.
	h.workers = append(h.workers, &tcpWorker{stream: &internet.MemoryStreamConfig{ProtocolName: "grpc"}})
	if err := m.DrainHandler(context.Background(), h.tag); err != errDrainUnsupported {
		t.Fatalf("unsupported transport: %v", err)
	}
	conn, err := stdnet.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal("preflight partially drained another listener")
	}
	defer conn.Close()
	echo(t, conn, "still-listening")
	if p.closed.Load() != 0 {
		t.Fatal("preflight changed proxy")
	}
}

func TestUDPDrainPreservesAssociationAndRejectsNewSources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &echoInbound{cancel: cancel}
	stream, err := internet.ToMemoryStreamConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	w := &udpWorker{address: xnet.LocalHostIP, proxy: p, stream: stream, ctx: context.WithValue(ctx, "cone", false)}
	l, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.LocalAddr().String()
	w.port = xnet.Port(l.LocalAddr().(*stdnet.UDPAddr).Port)
	l.Close()
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	first, err := stdnet.Dial("udp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	echo(t, first, "before")
	if err := w.Drain(); err != nil {
		t.Fatal(err)
	}
	echo(t, first, "existing-udp")
	second, err := stdnet.Dial("udp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	second.SetDeadline(time.Now().Add(120 * time.Millisecond))
	second.Write([]byte("blocked"))
	if _, err := second.Read(make([]byte, 32)); err == nil {
		t.Fatal("new UDP association accepted")
	}
	if err := w.Resume(); err != nil {
		t.Fatal(err)
	}
	echo(t, second, "new-after-resume")
	if p.closed.Load() != 0 {
		t.Fatal("UDP drain reset proxy")
	}
}

func TestRemovedOrUnstartedHandlerCannotResume(t *testing.T) {
	m, h, _, _ := tcpFixture(t)
	if err := m.RemoveHandler(context.Background(), h.tag); err != nil {
		t.Fatal(err)
	}
	if err := m.ResumeHandler(context.Background(), h.tag); err == nil {
		t.Fatal("removed handler resumed")
	}
	if err := h.Resume(); err != errNotRunning {
		t.Fatal("closed handler resumed")
	}
	if err := (&AlwaysOnInboundHandler{}).Drain(); err != errNotRunning {
		t.Fatal("unstarted handler drained")
	}
}

func TestUnixDrainPreservesStreamAndReopensSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &echoInbound{cancel: cancel}
	directory, err := os.MkdirTemp("/tmp", "xrd-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	address := filepath.Join(directory, "inbound.sock")
	stream, err := internet.ToMemoryStreamConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	w := &dsWorker{address: xnet.DomainAddress(address), proxy: p, stream: stream, ctx: ctx}
	h := &AlwaysOnInboundHandler{tag: "unix", proxy: p, workers: []worker{w}, mux: &mux.Server{}}
	if err := h.Start(); err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	conn, err := stdnet.DialTimeout("unix", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	echo(t, conn, "before")
	if err := h.Drain(); err != nil {
		t.Fatal(err)
	}
	if incoming, err := stdnet.DialTimeout("unix", address, time.Second); err == nil {
		incoming.Close()
		t.Fatal("new Unix connection accepted after drain")
	}
	echo(t, conn, "during")
	if err := h.Resume(); err != nil {
		t.Fatal(err)
	}
	newConn, err := stdnet.DialTimeout("unix", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer newConn.Close()
	echo(t, newConn, "new-after-resume")
	echo(t, conn, "old-after-resume")
	if p.closed.Load() != 0 {
		t.Fatal("Unix drain closed proxy state")
	}
}
