// Package accesslog 是 ipipx 魔改：xray 进程内【有界访问聚合器】（数据面 #1）。
//
// 数据流（access-log-final-design.md 方案 c）：
//
//	dispatcher 嗅探/目标确定 → emitAccessForConn（包级 hook，本 app Init 时注册）
//	  → Aggregator.Collect 有界 channel（满则丢 + dropped++，绝不阻塞转发）
//	  → 后台 goroutine 按 (email, domain, ip) 聚合进窗口（单 goroutine 独占，无锁）
//	  → app/accesslog/command 的 PullAccess gRPC 拉取（reset 读后清空）
//
// 进程内归属（区别于旧 node-agent 进程内 hook：分离部署收零事件）：聚合器在 xray 进程，
// 由 dispatcher hook 进程内直接喂入，永不跨进程，热路径只一次非阻塞 channel send。
//
// 铁律：
//   - 绝不阻塞转发：Collect 只做一次非阻塞 channel send，满即丢弃并计数。
//   - 不伪造字节数：up/down_bytes 恒 0（连接建立时未知；站点级字节 v1 不采，由 #2 stats 给总量）。
//   - 内存硬上界：单 email 站点上限 MaxSitesPerEmail，溢出 dropped++。
package accesslog

import (
	"sort"
	"sync/atomic"

	pb "github.com/xtls/xray-core/app/accesslog/command"
)

// Event 是一次访问事件（与 dispatcher.AccessEvent 同形，本包不直接 import dispatcher 以避免环依赖）。
type Event struct {
	Email    string
	Domain   string
	DestIP   string
	Protocol string
	UnixSec  int64
}

type siteKey struct {
	domain string
	ip     string
}

type siteAgg struct {
	count     uint64
	firstSeen int64
	lastSeen  int64
}

// DefaultChanSize / DefaultMaxSitesPerEmail 是聚合器默认容量与背压上限。
const (
	DefaultChanSize         = 4096
	DefaultMaxSitesPerEmail = 2000
)

// Aggregator 聚合访问事件成可上报的 AccessReport。
// 单后台 goroutine（run）独占窗口 map，无锁；Collect 仅向 channel 投递，故并发安全。
// snapshot 由 PullAccess（gRPC handler）调用：与 run goroutine 通过 reqs channel 同步，
// 保证 window 仍单 goroutine 独占（无锁）。
type Aggregator struct {
	maxSites int
	events   chan Event
	reqs     chan pullReq
	dropped  atomic.Uint64
	window   map[string]map[siteKey]*siteAgg
}

type pullReq struct {
	reset bool
	topN  int
	resp  chan *pb.AccessReport
}

// New 构造聚合器；chanSize/maxSites <= 0 回退默认。
func New(chanSize, maxSites int) *Aggregator {
	if chanSize <= 0 {
		chanSize = DefaultChanSize
	}
	if maxSites <= 0 {
		maxSites = DefaultMaxSitesPerEmail
	}
	return &Aggregator{
		maxSites: maxSites,
		events:   make(chan Event, chanSize),
		reqs:     make(chan pullReq),
		window:   make(map[string]map[siteKey]*siteAgg),
	}
}

// Collect 非阻塞投递一条事件。channel 满即丢弃并 dropped++（背压不阻塞转发路径）。
// 无 email 的事件直接丢弃（无归属实例，无法上报），不计 dropped。
func (a *Aggregator) Collect(ev Event) {
	if ev.Email == "" {
		return
	}
	select {
	case a.events <- ev:
	default:
		a.dropped.Add(1)
	}
}

// Run 跑聚合主循环（单 goroutine 独占窗口）。ctx-less：xray app 生命周期内常驻，进程退出即止。
func (a *Aggregator) Run() {
	for {
		select {
		case ev := <-a.events:
			a.ingest(ev)
		case req := <-a.reqs:
			req.resp <- a.snapshot(req.reset, req.topN)
		}
	}
}

// Pull 由 gRPC handler 调：向 run goroutine 请求一次快照（线程安全，window 仍单 goroutine 独占）。
func (a *Aggregator) Pull(reset bool, topN int) *pb.AccessReport {
	resp := make(chan *pb.AccessReport, 1)
	a.reqs <- pullReq{reset: reset, topN: topN, resp: resp}
	return <-resp
}

func (a *Aggregator) ingest(ev Event) {
	sites := a.window[ev.Email]
	if sites == nil {
		sites = make(map[siteKey]*siteAgg)
		a.window[ev.Email] = sites
	}
	k := siteKey{domain: ev.Domain, ip: ev.DestIP}
	if s, ok := sites[k]; ok {
		s.count++
		if ev.UnixSec < s.firstSeen {
			s.firstSeen = ev.UnixSec
		}
		if ev.UnixSec > s.lastSeen {
			s.lastSeen = ev.UnixSec
		}
		return
	}
	if len(sites) >= a.maxSites {
		a.dropped.Add(1) // 站点上限溢出：显式计入丢弃
		return
	}
	sites[k] = &siteAgg{count: 1, firstSeen: ev.UnixSec, lastSeen: ev.UnixSec}
}

// snapshot 取出当前窗口为 AccessReport；reset=true 时清空窗口并取走 dropped。
// topN>0 时每 email 按 count 降序留 topN，长尾折叠进 domain="(other)" 一条。
func (a *Aggregator) snapshot(reset bool, topN int) *pb.AccessReport {
	dropped := a.dropped.Load()
	if reset {
		dropped = a.dropped.Swap(0)
	}
	if len(a.window) == 0 && dropped == 0 {
		return &pb.AccessReport{}
	}
	report := &pb.AccessReport{Dropped: dropped}
	for email, sites := range a.window {
		visits := make([]*pb.SiteVisit, 0, len(sites))
		for k, s := range sites {
			visits = append(visits, &pb.SiteVisit{
				Domain:    k.domain,
				Ip:        k.ip,
				Count:     s.count,
				FirstSeen: uint64(s.firstSeen),
				LastSeen:  uint64(s.lastSeen),
				UpBytes:   0,
				DownBytes: 0,
			})
		}
		if topN > 0 && len(visits) > topN {
			visits = foldTopN(visits, topN)
		}
		report.Instances = append(report.Instances, &pb.InstanceAccess{Email: email, Sites: visits})
	}
	if reset {
		a.window = make(map[string]map[siteKey]*siteAgg)
	}
	return report
}

// foldTopN 按 count 降序保留 topN，其余折叠进一条 domain="(other)"（次数累加，时间取并集）。
func foldTopN(visits []*pb.SiteVisit, topN int) []*pb.SiteVisit {
	sort.Slice(visits, func(i, j int) bool { return visits[i].Count > visits[j].Count })
	other := &pb.SiteVisit{Domain: "(other)"}
	for _, v := range visits[topN:] {
		other.Count += v.Count
		if other.FirstSeen == 0 || (v.FirstSeen != 0 && v.FirstSeen < other.FirstSeen) {
			other.FirstSeen = v.FirstSeen
		}
		if v.LastSeen > other.LastSeen {
			other.LastSeen = v.LastSeen
		}
	}
	out := make([]*pb.SiteVisit, 0, topN+1)
	out = append(out, visits[:topN]...)
	out = append(out, other)
	return out
}
