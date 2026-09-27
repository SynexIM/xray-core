// Command outboundratelimit runs a real-process acceptance test for the
// aggregate outbound rate limiter. It intentionally uses an external Xray
// binary and real TCP/VLESS/SOCKS/HTTP traffic instead of in-process mocks.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/proxy"
)

const (
	cappedCustomers       = 100
	initialRateBitPerSec  = uint64(8_000_000)
	hotStartBitPerSec     = uint64(4_000_000)
	hotUpdatedBitPerSec   = uint64(16_000_000)
	parallelPayloadBytes  = int64(8 * 1024 * 1024)
	warmupPayloadBytes    = int64(512 * 1024)
	hotUpdatePayloadBytes = int64(8 * 1024 * 1024)
)

type downloadResult struct {
	Bytes             int64 `json:"bytes"`
	DurationMS        int64 `json:"durationMs"`
	MeasuredBitPerSec int64 `json:"measuredBitPerSec"`
}

type hotUpdateEvidence struct {
	FromBitPerSec              uint64 `json:"fromBitPerSec"`
	ToBitPerSec                uint64 `json:"toBitPerSec"`
	UpdateAfterMS              int64  `json:"updateAfterMs"`
	Bytes                      int64  `json:"bytes"`
	DurationMS                 int64  `json:"durationMs"`
	PIDBefore                  int    `json:"pidBefore"`
	PIDAfter                   int    `json:"pidAfter"`
	ExistingConnectionComplete bool   `json:"existingConnectionComplete"`
}

type evidence struct {
	BinarySHA256              string            `json:"binarySha256"`
	XrayVersion               string            `json:"xrayVersion"`
	ServerPID                 int               `json:"serverPid"`
	CustomersOnCappedOutbound int               `json:"customersOnCappedOutbound"`
	CapBitPerSec              uint64            `json:"capBitPerSec"`
	Capped                    downloadResult    `json:"capped"`
	Independent               downloadResult    `json:"independent"`
	HotUpdate                 hotUpdateEvidence `json:"hotUpdate"`
	Passed                    bool              `json:"passed"`
}

type process struct {
	cmd  *exec.Cmd
	logs *lockedBuffer
}

type lockedBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.b.Len()+len(p) > 64*1024 {
		value := b.b.String()
		b.b.Reset()
		if len(value) > 32*1024 {
			_, _ = b.b.WriteString(value[len(value)-32*1024:])
		}
	}
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func main() {
	var xrayBin string
	flag.StringVar(&xrayBin, "xray-bin", os.Getenv("XRAY_BIN"), "path to the Xray binary under test")
	flag.Parse()
	if strings.TrimSpace(xrayBin) == "" {
		fail(errors.New("-xray-bin or XRAY_BIN is required"))
	}
	if err := run(xrayBin); err != nil {
		fail(err)
	}
}

func run(xrayBin string) error {
	absoluteBin, err := filepath.Abs(xrayBin)
	if err != nil {
		return fmt.Errorf("resolve Xray binary: %w", err)
	}
	binaryHash, err := fileSHA256(absoluteBin)
	if err != nil {
		return fmt.Errorf("hash Xray binary: %w", err)
	}
	version, err := commandOutput(absoluteBin, "version")
	if err != nil {
		return fmt.Errorf("read Xray version: %w", err)
	}

	workdir, err := os.MkdirTemp("", "xray-outbound-rate-acceptance-")
	if err != nil {
		return fmt.Errorf("create work directory: %w", err)
	}
	defer os.RemoveAll(workdir)

	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen for HTTP target: %w", err)
	}
	targetServer := &http.Server{Handler: http.HandlerFunc(byteTarget)}
	go func() {
		_ = targetServer.Serve(targetListener)
	}()
	defer targetServer.Shutdown(context.Background())

	serverPort, apiPort, cappedSOCKSPort, independentSOCKSPort, err := fourFreePorts()
	if err != nil {
		return err
	}
	serverConfig := buildServerConfig(serverPort, apiPort)
	cappedClientConfig := buildClientConfig(serverPort, cappedSOCKSPort, userID(1))
	independentClientConfig := buildClientConfig(
		serverPort,
		independentSOCKSPort,
		userID(cappedCustomers+1),
	)
	serverConfigPath, err := writeConfig(workdir, "server.json", serverConfig)
	if err != nil {
		return err
	}
	cappedConfigPath, err := writeConfig(workdir, "capped-client.json", cappedClientConfig)
	if err != nil {
		return err
	}
	independentConfigPath, err := writeConfig(
		workdir,
		"independent-client.json",
		independentClientConfig,
	)
	if err != nil {
		return err
	}

	server, err := startXray(absoluteBin, serverConfigPath)
	if err != nil {
		return err
	}
	defer server.stop()
	if err := waitForPort(serverPort, server, 10*time.Second); err != nil {
		return fmt.Errorf("server VLESS inbound: %w", err)
	}
	if err := waitForPort(apiPort, server, 10*time.Second); err != nil {
		return fmt.Errorf("server API inbound: %w", err)
	}

	cappedClient, err := startXray(absoluteBin, cappedConfigPath)
	if err != nil {
		return err
	}
	defer cappedClient.stop()
	independentClient, err := startXray(absoluteBin, independentConfigPath)
	if err != nil {
		return err
	}
	defer independentClient.stop()
	if err := waitForPort(cappedSOCKSPort, cappedClient, 10*time.Second); err != nil {
		return fmt.Errorf("capped client SOCKS inbound: %w", err)
	}
	if err := waitForPort(independentSOCKSPort, independentClient, 10*time.Second); err != nil {
		return fmt.Errorf("independent client SOCKS inbound: %w", err)
	}

	targetURL := "http://" + targetListener.Addr().String() + "/bytes"
	if _, err := download(targetURL, cappedSOCKSPort, warmupPayloadBytes); err != nil {
		return fmt.Errorf(
			"drain initial capped burst: %w; server logs: %s; client logs: %s",
			err,
			server.diagnostic(),
			cappedClient.diagnostic(),
		)
	}

	start := make(chan struct{})
	var capped, independent downloadResult
	var cappedErr, independentErr error
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		<-start
		capped, cappedErr = download(targetURL, cappedSOCKSPort, parallelPayloadBytes)
	}()
	go func() {
		defer group.Done()
		<-start
		independent, independentErr = download(
			targetURL,
			independentSOCKSPort,
			parallelPayloadBytes,
		)
	}()
	close(start)
	group.Wait()
	if cappedErr != nil {
		return fmt.Errorf("capped outbound download: %w", cappedErr)
	}
	if independentErr != nil {
		return fmt.Errorf("independent outbound download: %w", independentErr)
	}
	if capped.MeasuredBitPerSec > int64(float64(initialRateBitPerSec)*1.03) {
		return fmt.Errorf(
			"capped outbound measured %d bit/s, exceeds 3%% tolerance around %d bit/s",
			capped.MeasuredBitPerSec,
			initialRateBitPerSec,
		)
	}
	if independent.DurationMS >= capped.DurationMS/3 {
		return fmt.Errorf(
			"independent outbound took %d ms while capped took %d ms; isolation not demonstrated",
			independent.DurationMS,
			capped.DurationMS,
		)
	}

	if err := setRate(absoluteBin, apiPort, hotStartBitPerSec); err != nil {
		return fmt.Errorf("set initial hot-test rate: %w", err)
	}
	pidBefore := server.cmd.Process.Pid
	hotStarted := time.Now()
	hotDone := make(chan struct{})
	var hot downloadResult
	var hotErr error
	go func() {
		defer close(hotDone)
		hot, hotErr = download(targetURL, cappedSOCKSPort, hotUpdatePayloadBytes)
	}()
	time.Sleep(3 * time.Second)
	updateAfter := time.Since(hotStarted)
	if err := setRate(absoluteBin, apiPort, hotUpdatedBitPerSec); err != nil {
		return fmt.Errorf("hot-update active outbound: %w", err)
	}
	<-hotDone
	if hotErr != nil {
		return fmt.Errorf("active connection after hot update: %w", hotErr)
	}
	pidAfter := server.cmd.Process.Pid
	if pidAfter != pidBefore || !server.alive() {
		return fmt.Errorf("Xray process changed during hot update: before=%d after=%d", pidBefore, pidAfter)
	}
	if hot.Bytes != hotUpdatePayloadBytes {
		return fmt.Errorf(
			"active connection returned %d bytes after hot update, want %d",
			hot.Bytes,
			hotUpdatePayloadBytes,
		)
	}
	if hot.DurationMS >= 12_000 {
		return fmt.Errorf(
			"hot-updated transfer took %d ms; 16 Mbit/s update did not affect the active transfer",
			hot.DurationMS,
		)
	}

	result := evidence{
		BinarySHA256:              binaryHash,
		XrayVersion:               firstLine(version),
		ServerPID:                 pidBefore,
		CustomersOnCappedOutbound: cappedCustomers,
		CapBitPerSec:              initialRateBitPerSec,
		Capped:                    capped,
		Independent:               independent,
		HotUpdate: hotUpdateEvidence{
			FromBitPerSec:              hotStartBitPerSec,
			ToBitPerSec:                hotUpdatedBitPerSec,
			UpdateAfterMS:              updateAfter.Milliseconds(),
			Bytes:                      hot.Bytes,
			DurationMS:                 hot.DurationMS,
			PIDBefore:                  pidBefore,
			PIDAfter:                   pidAfter,
			ExistingConnectionComplete: true,
		},
		Passed: true,
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode evidence: %w", err)
	}
	fmt.Println(string(encoded))
	return nil
}

func buildServerConfig(serverPort, apiPort int) map[string]any {
	cappedUsers := make([]map[string]any, 0, cappedCustomers)
	cappedEmails := make([]string, 0, cappedCustomers)
	for index := 1; index <= cappedCustomers; index++ {
		email := userEmail(index)
		cappedUsers = append(cappedUsers, map[string]any{"id": userID(index), "email": email})
		cappedEmails = append(cappedEmails, email)
	}
	independentIndex := cappedCustomers + 1
	allUsers := append(cappedUsers, map[string]any{
		"id":    userID(independentIndex),
		"email": userEmail(independentIndex),
	})
	return map[string]any{
		"log": map[string]any{"loglevel": "warning"},
		"api": map[string]any{
			"tag":      "api",
			"services": []string{"HandlerService"},
		},
		"inbounds": []map[string]any{
			{
				"tag":      "api",
				"listen":   "127.0.0.1",
				"port":     apiPort,
				"protocol": "dokodemo-door",
				"settings": map[string]any{"address": "127.0.0.1"},
			},
			{
				"tag":      "customers",
				"listen":   "127.0.0.1",
				"port":     serverPort,
				"protocol": "vless",
				"settings": map[string]any{
					"clients":    allUsers,
					"decryption": "none",
				},
			},
		},
		"outbounds": []map[string]any{
			{
				"tag":      "capped",
				"protocol": "freedom",
				"settings": map[string]any{
					"finalRules": []map[string]any{{"action": "allow"}},
				},
				"rateLimitBitPerSec": initialRateBitPerSec,
			},
			{
				"tag":      "independent",
				"protocol": "freedom",
				"settings": map[string]any{
					"finalRules": []map[string]any{{"action": "allow"}},
				},
			},
			{
				"tag":      "api",
				"protocol": "freedom",
				"settings": map[string]any{},
			},
		},
		"routing": map[string]any{
			"domainStrategy": "AsIs",
			"rules": []map[string]any{
				{"type": "field", "inboundTag": []string{"api"}, "outboundTag": "api"},
				{"type": "field", "user": cappedEmails, "outboundTag": "capped"},
				{
					"type":        "field",
					"user":        []string{userEmail(independentIndex)},
					"outboundTag": "independent",
				},
			},
		},
	}
}

func buildClientConfig(serverPort, socksPort int, id string) map[string]any {
	return map[string]any{
		"log": map[string]any{"loglevel": "warning"},
		"inbounds": []map[string]any{
			{
				"tag":      "socks",
				"listen":   "127.0.0.1",
				"port":     socksPort,
				"protocol": "socks",
				"settings": map[string]any{"auth": "noauth", "udp": false},
			},
		},
		"outbounds": []map[string]any{
			{
				"tag":      "server",
				"protocol": "vless",
				"settings": map[string]any{
					"vnext": []map[string]any{
						{
							"address": "127.0.0.1",
							"port":    serverPort,
							"users": []map[string]any{
								{"id": id, "encryption": "none"},
							},
						},
					},
				},
			},
		},
	}
}

func byteTarget(writer http.ResponseWriter, request *http.Request) {
	size, err := strconv.ParseInt(request.URL.Query().Get("n"), 10, 64)
	if err != nil || size < 0 || size > 64*1024*1024 {
		http.Error(writer, "invalid byte count", http.StatusBadRequest)
		return
	}
	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	chunk := make([]byte, 64*1024)
	for remaining := size; remaining > 0; {
		next := int64(len(chunk))
		if remaining < next {
			next = remaining
		}
		if _, err := writer.Write(chunk[:next]); err != nil {
			return
		}
		remaining -= next
	}
}

func download(baseURL string, socksPort int, size int64) (downloadResult, error) {
	dialer, err := proxy.SOCKS5(
		"tcp",
		net.JoinHostPort("127.0.0.1", strconv.Itoa(socksPort)),
		nil,
		proxy.Direct,
	)
	if err != nil {
		return downloadResult{}, err
	}
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return dialer.Dial(network, address)
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 45 * time.Second}
	started := time.Now()
	response, err := client.Get(baseURL + "?n=" + strconv.FormatInt(size, 10))
	if err != nil {
		return downloadResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return downloadResult{}, fmt.Errorf("HTTP target returned %s", response.Status)
	}
	count, err := io.Copy(io.Discard, response.Body)
	elapsed := time.Since(started)
	if err != nil {
		return downloadResult{}, err
	}
	if count != size {
		return downloadResult{}, fmt.Errorf("downloaded %d bytes, want %d", count, size)
	}
	return downloadResult{
		Bytes:             count,
		DurationMS:        elapsed.Milliseconds(),
		MeasuredBitPerSec: int64(float64(count*8) / elapsed.Seconds()),
	}, nil
}

func setRate(xrayBin string, apiPort int, bitPerSec uint64) error {
	output, err := commandOutput(
		xrayBin,
		"api",
		"outboundratelimit",
		"--server=127.0.0.1:"+strconv.Itoa(apiPort),
		"--tag=capped",
		"--bit-per-sec="+strconv.FormatUint(bitPerSec, 10),
	)
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(output))
	}
	return nil
}

func startXray(xrayBin, configPath string) (*process, error) {
	logs := &lockedBuffer{}
	cmd := exec.Command(xrayBin, "run", "-config", configPath)
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start Xray with %s: %w", filepath.Base(configPath), err)
	}
	return &process{cmd: cmd, logs: logs}, nil
}

func (p *process) alive() bool {
	return p != nil && p.cmd != nil && p.cmd.Process != nil &&
		p.cmd.Process.Signal(syscall.Signal(0)) == nil
}

func (p *process) diagnostic() string {
	if p == nil || p.logs == nil {
		return "(unavailable)"
	}
	value := strings.TrimSpace(p.logs.String())
	if value == "" {
		return "(empty)"
	}
	return value
}

func (p *process) stop() {
	if !p.alive() {
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() {
		_ = p.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
}

func waitForPort(port int, process *process, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			return nil
		}
		if !process.alive() {
			return fmt.Errorf("Xray exited: %s", strings.TrimSpace(process.logs.String()))
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s; logs: %s", address, strings.TrimSpace(process.logs.String()))
}

func fourFreePorts() (int, int, int, int, error) {
	ports := make([]int, 0, 4)
	for len(ports) < 4 {
		port, err := freePort()
		if err != nil {
			return 0, 0, 0, 0, err
		}
		duplicate := false
		for _, existing := range ports {
			if existing == port {
				duplicate = true
				break
			}
		}
		if !duplicate {
			ports = append(ports, port)
		}
	}
	return ports[0], ports[1], ports[2], ports[3], nil
}

func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("reserve free port: %w", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func writeConfig(workdir, name string, value map[string]any) (string, error) {
	path := filepath.Join(workdir, name)
	content, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode %s: %w", name, err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return "", fmt.Errorf("write %s: %w", name, err)
	}
	return path, nil
}

func userID(index int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", index)
}

func userEmail(index int) string {
	return fmt.Sprintf("customer-%03d@acceptance.invalid", index)
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func commandOutput(name string, args ...string) (string, error) {
	output, err := exec.Command(name, args...).CombinedOutput()
	return string(output), err
}

func firstLine(value string) string {
	if line, _, ok := strings.Cut(strings.TrimSpace(value), "\n"); ok {
		return line
	}
	return strings.TrimSpace(value)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "outbound rate acceptance failed:", err)
	os.Exit(1)
}
