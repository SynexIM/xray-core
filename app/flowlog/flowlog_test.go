package flowlog_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/flowlog"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf"
	_ "github.com/xtls/xray-core/main/distro/all"
	xproxy "golang.org/x/net/proxy"
)

// 审计门禁（真实 Xray）：一条经 SOCKS 用户的连接 → 一条 final 记录，email、出站本机地址、字节数都对；
// 长连接按 interim 补中间记录，各窗口字节相加等于总量。
func TestFlowRecordsRealConnection(t *testing.T) {
	const body = 300_000
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			for i := 0; i < 25; i++ { // 2.5 秒，跨过两个 1 秒的中间窗口
				w.Write(make([]byte, 1000))
				w.(http.Flusher).Flush()
				time.Sleep(100 * time.Millisecond)
			}
			return
		}
		w.Write(make([]byte, body))
	}))
	defer source.Close()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "flow.jsonl")
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := free.Addr().(*net.TCPAddr).Port
	free.Close()
	input := fmt.Sprintf(`{
	  "inbounds":[{"tag":"entry","listen":"127.0.0.1","port":%d,"protocol":"socks",
	    "settings":{"auth":"password","accounts":[{"user":"u","pass":"p","email":"c123-ip"}]}}],
	  "outbounds":[{"tag":"egress","protocol":"freedom"}],
	  "flowLog":{"path":%q,"interimSeconds":1}}`, port, logPath)
	var config conf.Config
	if err := json.Unmarshal([]byte(input), &config); err != nil {
		t.Fatal(err)
	}
	native, err := config.Build()
	if err != nil {
		t.Fatal(err)
	}
	instance, err := core.New(native)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err != nil {
		t.Fatal(err)
	}
	dialer, err := xproxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", port), &xproxy.Auth{User: "u", Password: "p"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{Dial: dialer.Dial, DisableKeepAlives: true}}
	for _, path := range []string{"/", "/slow"} {
		resp, err := client.Get(source.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	time.Sleep(300 * time.Millisecond) // 等 dispatcher 收尾
	instance.Close()                   // 关掉时写协程把队列写完

	f, err := os.Open(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	byConn := map[string][]flowlog.Record{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r flowlog.Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		byConn[r.ConnID] = append(byConn[r.ConnID], r)
	}
	if len(byConn) != 2 {
		t.Fatalf("两条连接应有两组记录，得到 %d 组", len(byConn))
	}
	sawInterim := false
	for id, recs := range byConn {
		last := recs[len(recs)-1]
		var down uint64
		for _, r := range recs {
			down += r.Down
			sawInterim = sawInterim || !r.Final
		}
		if !last.Final || last.Email != "c123-ip" || last.Inbound != "entry" || last.Outbound != "egress" {
			t.Fatalf("%s：最后一条应为 final 且带 email / 入站 / 出站：%+v", id, last)
		}
		if !strings.HasPrefix(last.Local, "127.0.0.1:") || last.Target != strings.TrimPrefix(source.URL, "http://") {
			t.Fatalf("%s：出站本机地址或对端不对：local=%q target=%q", id, last.Local, last.Target)
		}
		if down < 25_000 {
			t.Fatalf("%s：下行字节 %d 太少", id, down)
		}
	}
	if !sawInterim {
		t.Fatal("2.5 秒的连接应有 1 秒一条的中间记录")
	}
}
