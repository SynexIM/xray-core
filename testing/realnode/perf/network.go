package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/testing/realnode/internal/runner"
	xproxy "golang.org/x/net/proxy"
)

const nodeAddress = "10.202.1.2:19000"
const targetAddress = "10.202.2.2:19001"

func serveTarget() error {
	udp, err := net.ListenPacket("udp", targetAddress)
	if err != nil {
		return err
	}
	defer udp.Close()
	go func() {
		buffer := make([]byte, 65535)
		for {
			count, address, err := udp.ReadFrom(buffer)
			if err != nil {
				return
			}
			if _, err := udp.WriteTo(buffer[:count], address); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return
			}
		}
	}()
	listener, err := net.Listen("tcp", targetAddress)
	if err != nil {
		return err
	}
	defer listener.Close()
	for {
		connection, err := listener.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer connection.Close()
			var command [1]byte
			if _, err := io.ReadFull(connection, command[:]); err != nil {
				return
			}
			switch command[0] {
			case 'E':
				if _, err := connection.Write(command[:]); err == nil {
					io.Copy(connection, connection)
				}
			case 'U':
				io.Copy(io.Discard, connection)
			case 'D':
				buffer := make([]byte, 32768)
				for {
					if _, err := connection.Write(buffer); err != nil {
						return
					}
				}
			}
		}()
	}
}

func socksConnection(target string) (net.Conn, error) {
	dialer, err := xproxy.SOCKS5("tcp", nodeAddress, &xproxy.Auth{User: "u1", Password: "local-test-password"}, &net.Dialer{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	return dialer.Dial("tcp", target)
}
func pingSamples(seconds int, firstByte bool) ([]float64, error) {
	var values []float64
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	var connection net.Conn
	var err error
	if !firstByte {
		connection, err = socksConnection(targetAddress)
		if err != nil {
			return nil, err
		}
		defer connection.Close()
	}
	buffer := make([]byte, 64)
	buffer[0] = 'E'
	for time.Now().Before(deadline) {
		start := time.Now()
		if firstByte {
			connection, err = socksConnection(targetAddress)
			if err != nil {
				return nil, err
			}
		}
		connection.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := connection.Write(buffer); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(connection, buffer); err != nil {
			return nil, err
		}
		values = append(values, float64(time.Since(start).Nanoseconds())/1e6)
		if firstByte {
			connection.Close()
		}
		time.Sleep(20 * time.Millisecond)
	}
	return values, nil
}

func loadStreams(count int, download bool) (func() uint64, func(), error) {
	var connections []net.Conn
	var bytes atomic.Uint64
	var wait sync.WaitGroup
	stop := func() {
		for _, connection := range connections {
			connection.Close()
		}
		wait.Wait()
	}
	for index := 0; index < count; index++ {
		connection, err := socksConnection(targetAddress)
		if err != nil {
			stop()
			return nil, nil, err
		}
		command := byte('U')
		if download {
			command = 'D'
		}
		if _, err := connection.Write([]byte{command}); err != nil {
			connection.Close()
			stop()
			return nil, nil, err
		}
		connections = append(connections, connection)
		wait.Add(1)
		go func() {
			defer wait.Done()
			buffer := make([]byte, 32768)
			for {
				var count int
				var err error
				if download {
					count, err = connection.Read(buffer)
				} else {
					count, err = connection.Write(buffer)
				}
				bytes.Add(uint64(count))
				if err != nil {
					return
				}
			}
		}()
	}
	return bytes.Load, stop, nil
}

func udpAssociation() (net.Conn, *net.UDPConn, *net.UDPAddr, error) {
	control, err := net.DialTimeout("tcp", nodeAddress, 5*time.Second)
	if err != nil {
		return nil, nil, nil, err
	}
	fail := func(err error) (net.Conn, *net.UDPConn, *net.UDPAddr, error) {
		control.Close()
		return nil, nil, nil, err
	}
	control.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := control.Write([]byte{5, 1, 2}); err != nil {
		return fail(err)
	}
	var response [2]byte
	if _, err := io.ReadFull(control, response[:]); err != nil {
		return fail(err)
	}
	if response != [2]byte{5, 2} {
		return fail(fmt.Errorf("SOCKS 方法 %v", response))
	}
	username, password := "u1", "local-test-password"
	request := append([]byte{1, byte(len(username))}, username...)
	request = append(request, byte(len(password)))
	request = append(request, password...)
	if _, err := control.Write(request); err != nil {
		return fail(err)
	}
	if _, err := io.ReadFull(control, response[:]); err != nil {
		return fail(err)
	}
	if response[1] != 0 {
		return fail(fmt.Errorf("SOCKS 认证 %v", response))
	}
	if _, err := control.Write([]byte{5, 3, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		return fail(err)
	}
	var reply [10]byte
	if _, err := io.ReadFull(control, reply[:]); err != nil {
		return fail(err)
	}
	if reply[1] != 0 || reply[3] != 1 {
		return fail(fmt.Errorf("SOCKS UDP 回复 %v", reply))
	}
	address := &net.UDPAddr{IP: net.IPv4(10, 202, 1, 2), Port: int(binary.BigEndian.Uint16(reply[8:]))}
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(10, 202, 1, 1)})
	if err != nil {
		return fail(err)
	}
	if err := udp.SetReadBuffer(32 * 1024 * 1024); err != nil {
		udp.Close()
		return fail(err)
	}
	control.SetDeadline(time.Time{})
	return control, udp, address, nil
}

func udpSamples(seconds int, burst bool) ([]measurement, error) {
	control, connection, address, err := udpAssociation()
	if err != nil {
		return nil, err
	}
	defer control.Close()
	defer connection.Close()
	count := seconds * 625
	interval := 1600 * time.Microsecond
	name := "udp_constant_6mbps"
	if burst {
		count = 200
		interval = 50 * time.Microsecond
		name = "udp_burst_200"
	}
	packet := make([]byte, 1210)
	copy(packet, []byte{0, 0, 0, 1, 10, 202, 2, 2, 74, 57})
	var wait sync.WaitGroup
	var arrivals []time.Time
	seen := make(map[uint32]bool)
	var receiveError error
	wait.Add(1)
	go func() {
		defer wait.Done()
		buffer := make([]byte, 65535)
		for {
			length, _, err := connection.ReadFromUDP(buffer)
			if err != nil {
				if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
					receiveError = err
				}
				return
			}
			if length < 14 {
				receiveError = fmt.Errorf("UDP 回包过短 %d", length)
				return
			}
			sequence := binary.BigEndian.Uint32(buffer[10:14])
			if !seen[sequence] {
				seen[sequence] = true
				arrivals = append(arrivals, time.Now())
			}
		}
	}()
	start := time.Now()
	for sequence := 0; sequence < count; sequence++ {
		deadline := start.Add(time.Duration(sequence) * interval)
		if remaining := time.Until(deadline); remaining > 0 {
			time.Sleep(remaining)
		}
		binary.BigEndian.PutUint32(packet[10:14], uint32(sequence))
		if _, err := connection.WriteToUDP(packet, address); err != nil {
			connection.Close()
			wait.Wait()
			return nil, err
		}
	}
	elapsed := time.Since(start)
	connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	wait.Wait()
	if receiveError != nil {
		return nil, receiveError
	}
	var jitter []float64
	for index := 1; index < len(arrivals); index++ {
		jitter = append(jitter, math.Abs(float64(arrivals[index].Sub(arrivals[index-1])-interval))/1e6)
	}
	return []measurement{scalar(name+"_sent", "packets", float64(count)), scalar(name+"_lost", "packets", float64(count-len(seen))), scalar(name+"_send_duration", "ms", float64(elapsed.Nanoseconds())/1e6), distribution(name+"_jitter", "ms", jitter)}, nil
}

func clientBenchmark(options options) ([]measurement, error) {
	switch options.source {
	case "ping":
		values, err := pingSamples(options.seconds, false)
		return []measurement{distribution("small_packet_unlimited", "ms", values)}, err
	case "loaded_ping":
		_, stop, err := loadStreams(1, false)
		if err != nil {
			return nil, err
		}
		defer stop()
		time.Sleep(time.Second)
		values, err := pingSamples(options.seconds, false)
		return []measurement{distribution("small_packet_loaded_2mbps", "ms", values)}, err
	case "web":
		values, err := pingSamples(options.seconds, true)
		return []measurement{distribution("web_single", "ms", values)}, err
	case "loaded_web":
		bytes, stop, err := loadStreams(300, true)
		if err != nil {
			return nil, err
		}
		defer stop()
		time.Sleep(2 * time.Second)
		before := bytes()
		start := time.Now()
		values, err := pingSamples(options.seconds, true)
		rate := float64(bytes()-before) * 8 / time.Since(start).Seconds() / 1e6
		return []measurement{distribution("web_300_downloads_20mbps", "ms", values), scalar("download_300_aggregate", "Mbps", rate)}, err
	case "udp":
		return udpSamples(options.seconds, false)
	case "burst":
		return udpSamples(options.seconds, true)
	case "connections":
		return connectionBenchmark(options)
	case "iperf":
		return iperfBenchmark(options.seconds)
	case "shallow", "tc_reference":
		return downloadBenchmark(options.seconds, options.source)
	default:
		return nil, fmt.Errorf("未知客户端场景 %s", options.source)
	}
}

func tcpBytes() (float64, float64, error) {
	data, err := exec.Command("ip", "netns", "exec", "perf-node", "ss", "-tin", "sport = :19000").Output()
	if err != nil {
		return 0, 0, err
	}
	var sent, retransmitted float64
	for _, field := range strings.Fields(string(data)) {
		name, value, found := strings.Cut(field, ":")
		if !found || (name != "bytes_sent" && name != "bytes_retrans") {
			continue
		}
		amount, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, 0, err
		}
		if name == "bytes_sent" {
			sent += amount
		} else {
			retransmitted += amount
		}
	}
	if sent == 0 {
		return 0, 0, fmt.Errorf("ss 没有发送字节；无法计算重传率")
	}
	return sent, retransmitted, nil
}

func downloadBenchmark(seconds int, scenario string) ([]measurement, error) {
	bytes, stop, err := loadStreams(1, true)
	if err != nil {
		return nil, err
	}
	defer stop()
	time.Sleep(2 * time.Second)
	sentBefore, retransmittedBefore, err := tcpBytes()
	if err != nil {
		return nil, err
	}
	previous := bytes()
	last := time.Now()
	var rates []float64
	for second := 0; second < seconds; second++ {
		time.Sleep(time.Second)
		now := time.Now()
		current := bytes()
		rates = append(rates, float64(current-previous)*8/now.Sub(last).Seconds()/1e6)
		previous = current
		last = now
	}
	sent, retransmitted, err := tcpBytes()
	if err != nil {
		return nil, err
	}
	if sent <= sentBefore {
		return nil, fmt.Errorf("TCP 未继续发送")
	}
	return []measurement{distribution(scenario+"_throughput", "Mbps", rates), scalar(scenario+"_retransmission", "%", (retransmitted-retransmittedBefore)*100/(sent-sentBefore))}, nil
}

func shallowNetwork(reference bool) error {
	commands := [][]string{
		{"ip", "netns", "exec", "perf-bottleneck", "tc", "qdisc", "replace", "dev", "perf-bc", "root", "netem", "rate", "21mbit", "limit", "20"},
		{"ip", "netns", "exec", "perf-node", "tc", "qdisc", "replace", "dev", "perf-nc", "root", "fq", "limit", "4096"},
	}
	if reference {
		commands[1] = append(commands[1], "maxrate", "20mbit")
	}
	for _, arguments := range commands {
		output, err := exec.Command(arguments[0], arguments[1:]...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("%v: %w: %s", arguments, err, output)
		}
	}
	return nil
}

func connectionBenchmark(options options) ([]measurement, error) {
	var count, failures atomic.Uint64
	var firstError string
	var errorLock sync.Mutex
	var wait sync.WaitGroup
	start := time.Now()
	deadline := start.Add(time.Duration(options.seconds) * time.Second)
	for worker := 0; worker < options.concurrency; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			dialer := net.Dialer{Timeout: 5 * time.Second, LocalAddr: &net.TCPAddr{IP: net.IPv4(10, 202, 1, byte(101+worker%64))}}
			request := make([]byte, 27)
			request[18] = 1
			binary.BigEndian.PutUint16(request[19:21], 19001)
			request[21] = 1
			copy(request[22:26], []byte{10, 202, 2, 2})
			request[26] = 'E'
			for time.Now().Before(deadline) {
				id := identity(int(count.Load()+uint64(worker)) % 1000000)
				copy(request[1:17], id[:])
				connection, err := dialer.Dial("tcp", "10.202.1.2:19002")
				if err == nil {
					connection.SetDeadline(time.Now().Add(5 * time.Second))
					_, err = connection.Write(request)
					if err == nil {
						var response [3]byte
						_, err = io.ReadFull(connection, response[:])
						if err == nil && response != [3]byte{0, 0, 'E'} {
							err = fmt.Errorf("VLESS 回复 %v", response)
						}
					}
					connection.Close()
				}
				if err != nil {
					failures.Add(1)
					errorLock.Lock()
					if firstError == "" {
						firstError = err.Error()
					}
					errorLock.Unlock()
				} else {
					count.Add(1)
				}
			}
		}(worker)
	}
	wait.Wait()
	row := scalar("new_connections_1000000", "connections/s", float64(count.Load())/time.Since(start).Seconds())
	row.Note = fmt.Sprintf("并发 %d；失败 %d %s", options.concurrency, failures.Load(), firstError)
	return []measurement{row, scalar("new_connection_failures", "count", float64(failures.Load()))}, nil
}

func iperfBenchmark(seconds int) ([]measurement, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:19020")
	if err != nil {
		return nil, err
	}
	defer listener.Close()
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer client.Close()
				server, err := socksConnection("10.202.2.2:5201")
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					return
				}
				defer server.Close()
				done := make(chan struct{})
				go func() { io.Copy(server, client); close(done) }()
				io.Copy(client, server)
				client.Close()
				server.Close()
				<-done
			}()
		}
	}()
	data, err := exec.Command("iperf3", "-c", "127.0.0.1", "-p", "19020", "-t", strconv.Itoa(seconds), "-J").Output()
	if err != nil {
		return nil, err
	}
	var result struct {
		End struct {
			SumReceived struct {
				BitsPerSecond float64 `json:"bits_per_second"`
			} `json:"sum_received"`
		} `json:"end"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	if result.Error != "" {
		return nil, fmt.Errorf("iperf3: %s", result.Error)
	}
	return []measurement{scalar("iperf_proxy_throughput", "Gbps", result.End.SumReceived.BitsPerSecond/1e9)}, nil
}

func cpuSeconds(processID int) (float64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", processID))
	if err != nil {
		return 0, err
	}
	_, rest, found := strings.Cut(string(data), ") ")
	if !found {
		return 0, fmt.Errorf("进程 stat 格式错误")
	}
	fields := strings.Fields(rest)
	user, err := strconv.ParseFloat(fields[11], 64)
	if err != nil {
		return 0, err
	}
	system, err := strconv.ParseFloat(fields[12], 64)
	return (user + system) / 100, err
}

func networkBenchmark(options options) ([]measurement, error) {
	directory, err := os.MkdirTemp("", "xray-perf-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target := exec.CommandContext(ctx, "ip", "netns", "exec", "perf-target", os.Args[0], "-mode", "target")
	target.Stderr = os.Stderr
	if err := target.Start(); err != nil {
		return nil, err
	}
	defer func() { target.Process.Kill(); target.Wait() }()
	iperf := exec.CommandContext(ctx, "ip", "netns", "exec", "perf-target", "iperf3", "-s")
	if err := iperf.Start(); err != nil {
		return nil, err
	}
	defer func() { iperf.Process.Kill(); iperf.Wait() }()
	accounts := make([]map[string]any, 1000000)
	for index := range accounts {
		id := identity(index)
		accounts[index] = map[string]any{"id": id.String(), "email": fmt.Sprintf("v%d", index)}
	}
	var rows []measurement
	scenarios := []string{"connections", "ping", "loaded_ping", "udp", "burst", "iperf", "web", "loaded_web", "shallow", "tc_reference"}
	if options.source != "" {
		scenarios = []string{options.source}
	}
	for _, scenario := range scenarios {
		config := map[string]any{"log": map[string]any{"loglevel": "none"}, "stats": map[string]any{}, "policy": map[string]any{"levels": map[string]any{"0": map[string]any{"statsUserUplink": true, "statsUserDownlink": true, "statsUserOnline": true}}}, "inbounds": []map[string]any{{"tag": "entry", "listen": "10.202.1.2", "port": 19000, "protocol": "socks", "settings": map[string]any{"auth": "password", "udp": true, "accounts": []map[string]any{{"user": "u1", "pass": "local-test-password", "email": "u1"}}}}}, "outbounds": []map[string]any{{"tag": "direct", "protocol": "freedom", "settings": map[string]any{"finalRules": []map[string]any{{"action": "allow", "ip": []string{"10.202.2.2/32"}, "port": "19001,5201"}}}}}}
		rate := uint64(0)
		switch scenario {
		case "loaded_ping":
			rate = 2000000
		case "udp":
			rate = 10000000
		case "burst":
			rate = 100000000
		case "loaded_web", "web":
			rate = 20000000
		case "shallow":
			rate = 20000000
		}
		if scenario == "shallow" || scenario == "tc_reference" {
			if err := shallowNetwork(scenario == "tc_reference"); err != nil {
				rows = append(rows, measurement{Name: scenario, Error: err.Error()})
				continue
			}
		}
		if rate > 0 || scenario == "connections" || scenario == "tc_reference" {
			profile := map[string]any{}
			if rate > 0 {
				profile["up"] = []map[string]any{{"rate": fmt.Sprintf("%dbps", rate)}}
				profile["down"] = []map[string]any{{"rate": fmt.Sprintf("%dbps", rate)}}
			}
			config["limit"] = map[string]any{"profiles": map[string]any{"p": profile}, "rules": []map[string]any{{"ruleTag": "shared", "inboundTag": []string{"entry", "vless-entry"}, "profile": "p"}}}
		}
		if scenario == "connections" {
			config["inbounds"] = append(config["inbounds"].([]map[string]any), map[string]any{"tag": "vless-entry", "listen": "10.202.1.2", "port": 19002, "protocol": "vless", "settings": map[string]any{"decryption": "none", "clients": accounts}})
		}
		fmt.Fprintln(os.Stderr, "网络场景", scenario)
		start := time.Now()
		inbounds := config["inbounds"].([]map[string]any)
		inbounds[0]["listen"] = "0.0.0.0"
		node, err := runner.StartInNamespace(ctx, options.binary, directory, "node", config, "127.0.0.1:19000", 120*time.Second, "perf-node")
		if err != nil {
			return nil, err
		}
		if scenario == "connections" {
			rows = append(rows, scalar("full_config_ready_1000000", "s", time.Since(start).Seconds()))
		}
		before, err := cpuSeconds(node.Command.Process.Pid)
		if err != nil {
			node.Stop()
			return nil, err
		}
		measureStart := time.Now()
		command := exec.CommandContext(ctx, "ip", "netns", "exec", "perf-client", os.Args[0], "-mode", "client", "-source", scenario, "-seconds", strconv.Itoa(options.seconds))
		command.Stderr = os.Stderr
		output, clientError := command.Output()
		after, cpuError := cpuSeconds(node.Command.Process.Pid)
		node.Stop()
		if clientError != nil {
			rows = append(rows, measurement{Name: scenario, Error: clientError.Error()})
			continue
		}
		if cpuError != nil {
			return nil, cpuError
		}
		var measured []measurement
		if err := json.Unmarshal(output, &measured); err != nil {
			return nil, err
		}
		rows = append(rows, measured...)
		cores := (after - before) / time.Since(measureStart).Seconds()
		if scenario == "connections" {
			rows = append(rows, scalar("new_connections_node_cpu", "% of 8C", cores*100/8))
		}
		if scenario == "iperf" {
			for _, row := range measured {
				if row.Name == "iperf_proxy_throughput" && row.Value != nil && *row.Value > 0 {
					rows = append(rows, scalar("cpu_per_gbps", "cores/Gbps", cores / *row.Value))
				}
			}
		}
	}
	rows = append(rows, measurement{Name: "hong_kong_node", Error: "生产节点未授权变更；本轮只运行隔离容器"})
	return rows, nil
}
