// 真实进程验收：客户端经 SOCKS 进入指定 Xray 二进制，速率在目的端量取。
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	handlercommand "github.com/xtls/xray-core/app/proxyman/command"
	statscommand "github.com/xtls/xray-core/app/stats/command"
	"github.com/xtls/xray-core/testing/realnode/internal/runner"
	xproxy "golang.org/x/net/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type options struct {
	binary, scenario, direction string
	seconds                     int
	unlimited                   bool
	debug                       bool
	sourceIPs                   string
}
type sample struct {
	rate       float64
	individual []float64
}
type stream struct {
	bytes atomic.Uint64
	ready chan struct{}
	once  sync.Once
}

func (s *stream) record(n int) { s.bytes.Add(uint64(n)); s.once.Do(func() { close(s.ready) }) }

type countWriter struct{ s *stream }

func (w countWriter) Write(p []byte) (int, error) { w.s.record(len(p)); return len(p), nil }

type infiniteReader struct{}

func (infiniteReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func startXray(ctx context.Context, binary, dir, name string, config any, readyAddress string) (*runner.Child, error) {
	return runner.Start(ctx, binary, dir, name, config, readyAddress, 8*time.Second)
}
func freePort() (int, error) { return runner.FreePort() }
func steps(rates ...uint64) []map[string]any {
	result := make([]map[string]any, 0, len(rates))
	for i, r := range rates {
		step := map[string]any{"rate": fmt.Sprintf("%dbps", r)}
		if i < len(rates)-1 {
			duration := "5s"
			if i == 1 {
				duration = "20s"
			}
			step["for"] = duration
		}
		result = append(result, step)
	}
	return result
}
func profile(rates ...uint64) map[string]any {
	return map[string]any{"up": steps(rates...), "down": steps(rates...)}
}
func limitConfig(scenario string, count int) map[string]any {
	names := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("u%d", i+1)
	}
	profiles := map[string]any{}
	var rules []map[string]any
	switch scenario {
	case "ladder":
		profiles["p"] = profile(10_000_000, 4_000_000, 1_000_000)
	case "shared":
		profiles["p"] = profile(8_000_000)
	case "outbound":
		profiles["p"] = profile(8_000_000)
		profiles["out"] = profile(20_000_000)
		for _, name := range names {
			rules = append(rules, map[string]any{"ruleTag": name, "user": []string{name}, "profile": "p"})
		}
		rules = append(rules, map[string]any{"ruleTag": "out", "outboundTag": []string{"direct"}, "profile": "out"})
	case "hotswap":
		profiles["p"] = profile(2_000_000)
	case "sourceips":
		p := profile(2_000_000)
		p["maxSourceIps"] = 2
		profiles["p"] = p
	case "vision":
		profiles["p"] = profile(5_000_000)
	}
	if scenario != "outbound" {
		rules = append(rules, map[string]any{"ruleTag": "shared", "user": names, "profile": "p"})
	}
	return map[string]any{"profiles": profiles, "rules": rules}
}
func httpClient(socksAddress, username, sourceIP string) (*http.Client, error) {
	forward := &net.Dialer{Timeout: 5 * time.Second}
	if sourceIP != "" {
		ip := net.ParseIP(sourceIP)
		if ip == nil {
			return nil, fmt.Errorf("非法来源地址 %q", sourceIP)
		}
		forward.LocalAddr = &net.TCPAddr{IP: ip}
	}
	var auth *xproxy.Auth
	if username != "" {
		auth = &xproxy.Auth{User: username, Password: "local-test-password"}
	}
	dialer, err := xproxy.SOCKS5("tcp", socksAddress, auth, forward)
	if err != nil {
		return nil, err
	}
	contextDialer, ok := dialer.(xproxy.ContextDialer)
	if !ok {
		return nil, errors.New("SOCKS dialer 缺少 context")
	}
	transport := &http.Transport{
		DialContext: contextDialer.DialContext, DisableKeepAlives: true,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true}, // 仅本机临时测试证书
		TLSHandshakeTimeout: 5 * time.Second,
	}
	return &http.Client{Transport: transport}, nil
}
func runStream(ctx context.Context, client *http.Client, url, direction string, s *stream, errs chan<- error, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer client.CloseIdleConnections()
		method := http.MethodGet
		var body io.Reader
		if direction == "upload" {
			method = http.MethodPost
			body = infiniteReader{}
		}
		request, err := http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			errs <- err
			return
		}
		response, err := client.Do(request)
		if err == nil {
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				err = fmt.Errorf("源站状态 %d", response.StatusCode)
			} else if direction == "download" {
				buffer := make([]byte, 32*1024)
				for {
					n, readErr := response.Body.Read(buffer)
					if n > 0 {
						s.record(n)
					}
					if readErr != nil {
						err = readErr
						break
					}
				}
			} else {
				_, err = io.Copy(io.Discard, response.Body)
			}
		}
		if ctx.Err() == nil {
			if err == nil {
				err = errors.New("流提前结束")
			}
			select {
			case errs <- err:
			case <-ctx.Done():
			}
		}
	}()
}
func waitReady(ctx context.Context, streams []*stream, errs <-chan error) error {
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	for _, s := range streams {
		select {
		case <-s.ready:
		case err := <-errs:
			return err
		case <-deadline.C:
			return errors.New("代理流没有在 8 秒内开始")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func probe(ctx context.Context, client *http.Client, url string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/probe", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("probe 状态 %d", response.StatusCode)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if string(data) != "ok" {
		return errors.New("probe 没经真实出站完成")
	}
	return nil
}
func swapProfile(ctx context.Context, binary, dir, socket string) error {
	path := filepath.Join(dir, "profiles.json")
	data, err := json.Marshal(map[string]any{"profiles": map[string]any{"p": profile(5_000_000)}})
	if err != nil {
		return err
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, binary, "api", "limit", "putprofiles", "-s", "unix:"+socket, path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("PutProfiles: %w\n%s", err, output)
	}
	return nil
}
func measure(ctx context.Context, streams []*stream, seconds int, errs <-chan error, onSecond func(int) error) ([]sample, error) {
	previous := make([]uint64, len(streams))
	for i, s := range streams {
		previous[i] = s.bytes.Load()
	}
	result := make([]sample, 0, seconds)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	last := time.Now()
	for second := 1; second <= seconds; second++ {
		select {
		case err := <-errs:
			return result, err
		case <-ctx.Done():
			return result, ctx.Err()
		case now := <-ticker.C:
			elapsed := now.Sub(last).Seconds()
			last = now
			row := sample{individual: make([]float64, len(streams))}
			for i, s := range streams {
				current := s.bytes.Load()
				row.individual[i] = float64(current-previous[i]) * 8 / elapsed / 1e6
				previous[i] = current
				row.rate += row.individual[i]
			}
			result = append(result, row)
			fmt.Printf("t=%ds rate=%.2fMbps\n", second, row.rate)
			if onSecond != nil {
				if err := onSecond(second); err != nil {
					return result, err
				}
			}
		}
	}
	return result, nil
}
func checkRate(rows []sample, start, end int, expected float64) error {
	if start < 0 || end > len(rows) || start >= end {
		return errors.New("实测窗口不足")
	}
	sum := 0.0
	for _, row := range rows[start:end] {
		sum += row.rate
	}
	average := sum / float64(end-start)
	fmt.Fprintf(os.Stderr, "窗口 %d–%ds: %.3fMbps，期望 %.3fMbps\n", start, end, average, expected)
	if math.Abs(average-expected) > expected*0.12 {
		return fmt.Errorf("窗口 %d–%ds 速率 %.3fMbps 不满足 %.3fMbps ±12%%", start, end, average, expected)
	}
	return nil
}
func validate(scenario string, rows []sample) error {
	switch scenario {
	case "ladder":
		if err := checkRate(rows, 0, 5, 10); err != nil {
			return err
		}
		if err := checkRate(rows, 6, 24, 4); err != nil {
			return err
		}
		if err := checkRate(rows, 27, len(rows), 1); err != nil {
			return err
		}
		for _, point := range []struct {
			at            int
			before, after float64
		}{{5, 10, 4}, {25, 4, 1}} {
			transition := -1
			for i, row := range rows {
				if row.rate < (point.before+point.after)/2 {
					transition = i + 1
					break
				}
			}
			if transition < point.at || transition > point.at+1 {
				return fmt.Errorf("拐点 %ds 实际在 %ds，误差超 1 秒", point.at, transition)
			}
		}
	case "shared":
		return checkRate(rows, 3, len(rows), 8)
	case "outbound":
		if err := checkRate(rows, 3, len(rows), 20); err != nil {
			return err
		}
		for _, row := range rows[3:] {
			for i, rate := range row.individual {
				if rate > 8.8 {
					return fmt.Errorf("第 %d 条用户桶超过 8Mbps: %.3fMbps", i+1, rate)
				}
			}
		}
	case "hotswap":
		if err := checkRate(rows, 1, 5, 2); err != nil {
			return err
		}
		if len(rows) < 8 {
			return errors.New("hotswap 需要至少 8 秒")
		}
		if math.Abs(rows[5].rate-5) > 1 {
			return fmt.Errorf("PutProfiles 后 1 秒没有变速: %.3fMbps", rows[5].rate)
		}
		return checkRate(rows, 6, len(rows), 5)
	case "vision":
		for second, row := range rows[3:] {
			if row.rate > 5.6 {
				return fmt.Errorf("Vision 第 %d 秒超过 5Mbps 上限: %.3fMbps", second+4, row.rate)
			}
		}
		return checkRate(rows, 3, len(rows), 5)
	}
	return nil
}
func visionConfig(serverPort, clientPort int, source *httptest.Server) (map[string]any, map[string]any, error) {
	certificate := source.TLS.Certificates[0]
	certHash := sha256.Sum256(certificate.Certificate[0])
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		return nil, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	identity := "00000000-0000-4000-8000-000000000001"
	inbound := map[string]any{"tag": "entry", "listen": "127.0.0.1", "port": serverPort, "protocol": "vless",
		"settings":       map[string]any{"decryption": "none", "clients": []map[string]any{{"id": identity, "email": "u1", "flow": "xtls-rprx-vision"}}},
		"streamSettings": map[string]any{"network": "raw", "security": "tls", "tlsSettings": map[string]any{"certificates": []map[string]any{{"certificate": strings.Split(strings.TrimSpace(string(certPEM)), "\n"), "key": strings.Split(strings.TrimSpace(string(keyPEM)), "\n")}}}},
	}
	client := map[string]any{
		"log":      map[string]any{"loglevel": "warning"},
		"inbounds": []map[string]any{{"listen": "127.0.0.1", "port": clientPort, "protocol": "socks", "settings": map[string]any{"auth": "noauth"}}},
		"outbounds": []map[string]any{{"protocol": "vless", "settings": map[string]any{"vnext": []map[string]any{{"address": "127.0.0.1", "port": serverPort, "users": []map[string]any{{"id": identity, "encryption": "none", "flow": "xtls-rprx-vision"}}}}},
			"streamSettings": map[string]any{"network": "raw", "security": "tls", "tlsSettings": map[string]any{"serverName": "example.com", "pinnedPeerCertSha256": hex.EncodeToString(certHash[:])}}}},
	}
	return inbound, client, nil
}
func statsValue(ctx context.Context, socket, name string) (uint64, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(rpcCtx, "unix:"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	result, err := statscommand.NewStatsServiceClient(conn).GetStats(rpcCtx, &statscommand.GetStatsRequest{Name: name})
	if err != nil {
		return 0, err
	}
	return uint64(result.GetStat().GetValue()), nil
}

// 本机停止上传后 TCP 缓冲仍可能继续供数，显式断会话才开始空闲回收验收。
func killConnections(ctx context.Context, socket string, emails []string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(rpcCtx, "unix:"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = handlercommand.NewHandlerServiceClient(conn).KillUserConnections(rpcCtx, &handlercommand.KillUserConnectionsRequest{Emails: emails})
	return err
}
func run(o options) error {
	if o.binary == "" {
		return errors.New("必须传 -xray 二进制路径")
	}
	binary, err := filepath.Abs(o.binary)
	if err != nil {
		return err
	}
	if o.direction != "upload" && o.direction != "download" {
		return errors.New("-direction 必须是 upload 或 download")
	}
	if o.seconds <= 0 {
		return errors.New("-seconds 必须大于零")
	}
	switch o.scenario {
	case "ladder", "shared", "outbound", "hotswap", "sourceips", "vision":
	default:
		return fmt.Errorf("未知场景 %q", o.scenario)
	}
	if !o.unlimited && o.scenario == "ladder" && o.seconds < 30 {
		return errors.New("ladder 至少测 30 秒；验收用 60 秒")
	}
	if !o.unlimited && (o.scenario == "shared" || o.scenario == "outbound" || o.scenario == "vision") && o.seconds < 5 {
		return errors.New("该场景至少测 5 秒")
	}
	if o.scenario == "hotswap" && o.seconds < 8 {
		return errors.New("hotswap 至少测 8 秒")
	}
	if o.scenario == "sourceips" && o.seconds < 35 {
		return errors.New("sourceips 至少测 35 秒")
	}
	if o.scenario == "vision" && o.direction != "download" {
		return errors.New("vision 使用 HTTPS 下载触发直通路径，请传 -direction download")
	}
	dir, err := os.MkdirTemp("", "xray-limit-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	count := 1
	if o.scenario == "shared" {
		count = 4
	}
	if o.scenario == "outbound" {
		count = 5
	}
	if o.scenario == "sourceips" {
		count = 2
	}
	streams := make([]*stream, count)
	for i := range streams {
		streams[i] = &stream{ready: make(chan struct{})}
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/probe" {
			io.WriteString(w, "ok")
			return
		}
		index, err := strconv.Atoi(request.URL.Query().Get("stream"))
		if err != nil || index < 0 || index >= len(streams) {
			http.Error(w, "invalid stream", 400)
			return
		}
		if request.Method == http.MethodPost {
			io.Copy(countWriter{streams[index]}, request.Body)
			return
		}
		buffer := make([]byte, 32*1024)
		flusher, _ := w.(http.Flusher)
		for {
			if _, err := w.Write(buffer); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	})
	var source *httptest.Server
	if o.scenario == "vision" {
		source = httptest.NewTLSServer(handler)
	} else {
		source = httptest.NewServer(handler)
	}
	defer source.Close()
	serverPort, err := freePort()
	if err != nil {
		return err
	}
	socket := filepath.Join(dir, "api.sock")
	accounts := make([]map[string]any, count)
	for i := range accounts {
		name := fmt.Sprintf("u%d", i+1)
		accounts[i] = map[string]any{"user": name, "pass": "local-test-password", "email": name}
	}
	inbound := map[string]any{"tag": "entry", "listen": "127.0.0.1", "port": serverPort, "protocol": "socks", "settings": map[string]any{"auth": "password", "accounts": accounts, "udp": true}}
	config := map[string]any{
		"log":       map[string]any{"loglevel": "warning"},
		"inbounds":  []map[string]any{inbound},
		"outbounds": []map[string]any{{"tag": "direct", "protocol": "freedom", "settings": map[string]any{}}},
		"stats":     map[string]any{},
		"policy":    map[string]any{"levels": map[string]any{"0": map[string]any{"statsUserUplink": true, "statsUserDownlink": true}}},
	}
	services := []string{"HandlerService", "StatsService"}
	if !o.unlimited {
		services = append(services, "LimitService")
	}
	config["api"] = map[string]any{"tag": "api", "listen": socket, "services": services}
	if o.debug {
		config["log"] = map[string]any{"loglevel": "debug"}
	}
	if !o.unlimited {
		config["limit"] = limitConfig(o.scenario, count)
	}
	// v26.10.10 默认保护私网目的地；验收只放行这一个本机源站端口。
	_, sourcePort, err := net.SplitHostPort(source.Listener.Addr().String())
	if err != nil {
		return err
	}
	config["outbounds"] = []map[string]any{{"tag": "direct", "protocol": "freedom", "settings": map[string]any{
		"finalRules": []map[string]any{{"action": "allow", "ip": []string{"127.0.0.1/32"}, "port": sourcePort}},
	}}}
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()
	socksAddress := fmt.Sprintf("127.0.0.1:%d", serverPort)
	var clientConfig map[string]any
	if o.scenario == "vision" {
		clientPort, err := freePort()
		if err != nil {
			return err
		}
		inbound, clientConfig, err = visionConfig(serverPort, clientPort, source)
		if err != nil {
			return err
		}
		config["inbounds"] = []map[string]any{inbound}
		if o.debug {
			clientConfig["log"] = map[string]any{"loglevel": "debug"}
		}
		socksAddress = fmt.Sprintf("127.0.0.1:%d", clientPort)
	}
	server, err := startXray(rootCtx, binary, dir, "server", config, fmt.Sprintf("127.0.0.1:%d", serverPort))
	if err != nil {
		return err
	}
	defer server.Stop()
	if clientConfig != nil {
		client, err := startXray(rootCtx, binary, dir, "client", clientConfig, socksAddress)
		if err != nil {
			return err
		}
		defer client.Stop()
	}
	measureCtx, measureCancel := context.WithCancel(rootCtx)
	defer measureCancel()
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	sourceIPs := strings.Split(o.sourceIPs, ",")
	var clients []*http.Client
	var individualCancels []context.CancelFunc
	for i, s := range streams {
		username := fmt.Sprintf("u%d", i+1)
		if o.scenario == "vision" {
			username = ""
		}
		sourceIP := ""
		if o.scenario == "sourceips" {
			if len(sourceIPs) < 3 {
				return errors.New("sourceips 需要三个 -source-ips")
			}
			sourceIP = strings.TrimSpace(sourceIPs[i])
		}
		client, err := httpClient(socksAddress, username, sourceIP)
		if err != nil {
			return err
		}
		clients = append(clients, client)
		streamCtx, streamCancel := context.WithCancel(measureCtx)
		individualCancels = append(individualCancels, streamCancel)
		runStream(streamCtx, client, source.URL+"?stream="+strconv.Itoa(i), o.direction, s, errs, &wg)
	}
	defer func() { measureCancel(); wg.Wait() }()
	if err = waitReady(measureCtx, streams, errs); err != nil {
		logs, _ := os.ReadFile(server.Log)
		clientLogs, _ := os.ReadFile(filepath.Join(dir, "client.log"))
		return fmt.Errorf("真实代理流启动失败: %w\nserver:\n%s\nclient:\n%s", err, logs, clientLogs)
	}
	var onSecond func(int) error
	if o.scenario == "hotswap" && !o.unlimited {
		onSecond = func(second int) error {
			if second == 5 {
				return swapProfile(rootCtx, binary, dir, socket)
			}
			return nil
		}
	}
	if o.scenario == "sourceips" && !o.unlimited {
		third, err := httpClient(socksAddress, "u1", strings.TrimSpace(sourceIPs[2]))
		if err != nil {
			return err
		}
		probeCtx, cancel := context.WithTimeout(rootCtx, 3*time.Second)
		rejected := probe(probeCtx, third, source.URL)
		cancel()
		if rejected == nil && !o.unlimited {
			return errors.New("第三个来源 IP 没有被拒绝")
		}
		fmt.Fprintf(os.Stderr, "第三个来源 IP 被拒绝: %v\n", rejected)
		onSecond = func(second int) error {
			if second == 10 {
				for _, cancel := range individualCancels {
					cancel()
				}
				if err := killConnections(rootCtx, socket, []string{"u1", "u2"}); err != nil {
					return fmt.Errorf("断开来源 IP 测试会话: %w", err)
				}
			}
			if second == 32 {
				probeCtx, cancel := context.WithTimeout(rootCtx, 3*time.Second)
				defer cancel()
				if err := probe(probeCtx, third, source.URL); err != nil {
					return fmt.Errorf("闲置超过 20 秒后第三个 IP 仍被拒绝: %w", err)
				}
				fmt.Fprintln(os.Stderr, "闲置超过 20 秒后第三个来源 IP 可用")
			}
			return nil
		}
	}
	rows, err := measure(measureCtx, streams, o.seconds, errs, onSecond)
	measureCancel()
	wg.Wait()
	if err != nil {
		logs, _ := os.ReadFile(server.Log)
		return fmt.Errorf("真实流测量失败: %w\n%s", err, logs)
	}
	if !o.unlimited {
		if err = validate(o.scenario, rows); err != nil {
			return err
		}
	}
	if o.scenario == "vision" {
		actual := streams[0].bytes.Load()
		counted, err := statsValue(rootCtx, socket, "user>>>u1>>>traffic>>>downlink")
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Vision 目的端字节=%d，用户统计字节=%d\n", actual, counted)
		if counted < actual || float64(counted-actual) > float64(actual)*0.08 {
			return errors.New("Vision 流量统计与真实传输不一致")
		}
	}
	fmt.Fprintf(os.Stderr, "PASS %s %s（真实 Xray 进程，%d 秒）\n", o.scenario, o.direction, len(rows))
	return nil
}
func main() {
	var o options
	flag.StringVar(&o.binary, "xray", "", "真实 xray 二进制路径")
	flag.StringVar(&o.scenario, "scenario", "ladder", "ladder/shared/outbound/hotswap/sourceips/vision")
	flag.StringVar(&o.direction, "direction", "upload", "upload/download；Vision 使用 download")
	flag.IntVar(&o.seconds, "seconds", 60, "测量秒数")
	flag.BoolVar(&o.unlimited, "unlimited", false, "不加载 limit，用于先测基线")
	flag.BoolVar(&o.debug, "debug", false, "失败时附加本机测试进程调试日志")
	flag.StringVar(&o.sourceIPs, "source-ips", "127.0.0.2,127.0.0.3,127.0.0.4", "sourceips 使用的本机来源地址")
	flag.Parse()
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}
