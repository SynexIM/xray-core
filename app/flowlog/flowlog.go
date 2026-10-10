// Package flowlog 为每条连接写审计记录（docs：ProxyAstra fork.md 第 3 节）。
//
// 每条记录是一个时间窗内的事实：连接结束时写一条 final；连着超过 interim 的，每隔 interim 补一条，
// 字节数都是本窗增量，下游直接求和即可。写出走有界通道 + 单个写协程，来不及就丢弃并计数——
// 审计绝不拖慢转发。
package flowlog

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/app/capability"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/session"
	featureflowlog "github.com/xtls/xray-core/features/flowlog"
)

const queueSize = 65536

// Record 是写出的一行 JSON。
type Record struct {
	ConnID      string `json:"connId"`
	ConnStart   int64  `json:"connStart"` // 毫秒
	WindowStart int64  `json:"windowStart"`
	WindowEnd   int64  `json:"windowEnd"`
	Final       bool   `json:"final"`
	Email       string `json:"email,omitempty"`
	Inbound     string `json:"inbound,omitempty"`
	Outbound    string `json:"outbound,omitempty"`
	Network     string `json:"network"`
	Source      string `json:"source,omitempty"`
	Domain      string `json:"domain,omitempty"`
	Target      string `json:"target,omitempty"` // 真正拨通的对端（没拨通时是请求的目标）
	Local       string `json:"local,omitempty"`  // 出站本机 IP:端口：网站投诉只给这个
	Up          uint64 `json:"up"`
	Down        uint64 `json:"down"`
	Result      string `json:"result,omitempty"` // 空 = 正常；否则拒绝 / 失败原因
}

type flow struct {
	id       string
	s        featureflowlog.Session
	start    time.Time
	up, down atomic.Uint64
	// 以下只在 Recorder.mu 下读写：上一窗口的结束时间与已写出的累计字节。
	windowStart            time.Time
	emittedUp, emittedDown uint64
}

type Recorder struct {
	interim time.Duration
	out     *rotator
	queue   chan []byte
	dropped atomic.Uint64
	prefix  string // 进程启动时刻：连接号在节点上唯一，进程重启也不撞
	seq     atomic.Uint64

	mu     sync.Mutex
	active map[*flow]struct{}

	stop chan struct{}
	done sync.WaitGroup
}

func New(c *Config) (*Recorder, error) {
	if c.GetPath() == "" {
		return nil, errors.New("flowlog: path 必填")
	}
	out, err := newRotator(c.GetPath(), int64(orDefault(c.GetMaxSizeMb(), 256))<<20, int(orDefault(c.GetMaxFiles(), 8)))
	if err != nil {
		return nil, err
	}
	r := &Recorder{
		interim: time.Duration(orDefault(c.GetInterimSeconds(), 60)) * time.Second,
		out:     out,
		queue:   make(chan []byte, queueSize),
		prefix:  time.Now().UTC().Format("20060102T150405") + "-",
		active:  map[*flow]struct{}{},
		stop:    make(chan struct{}),
	}
	return r, nil
}

func orDefault(v, d uint32) uint32 {
	if v == 0 {
		return d
	}
	return v
}

func (*Recorder) Type() interface{} { return featureflowlog.Type() }

func (r *Recorder) Start() error {
	r.done.Add(2)
	go r.write()
	go r.tick()
	return nil
}

func (r *Recorder) Close() error {
	close(r.stop)
	r.done.Wait()
	return r.out.Close()
}

// Dropped 是来不及写而丢弃的记录数（运维指标）。
func (r *Recorder) Dropped() uint64 { return r.dropped.Load() }

// Open 实现 features/flowlog.Recorder。
func (r *Recorder) Open(ctx context.Context, s featureflowlog.Session) (featureflowlog.WrapReader, featureflowlog.WrapWriter, func(string)) {
	// 走 splice 的字节绕过 link，计不到；审计的连接一律不 splice（与限速同一做法）。
	session.DisableSplice(ctx)
	now := time.Now()
	f := &flow{id: r.prefix + strconv.FormatUint(r.seq.Add(1), 10), s: s, start: now, windowStart: now}
	r.mu.Lock()
	r.active[f] = struct{}{}
	r.mu.Unlock()
	up := func(rd buf.Reader) buf.Reader { return &countingReader{Reader: rd, n: &f.up} }
	down := func(w buf.Writer) buf.Writer { return &countingWriter{Writer: w, n: &f.down} }
	var once sync.Once
	finish := func(result string) {
		once.Do(func() {
			r.mu.Lock()
			delete(r.active, f)
			rec := r.window(f, time.Now(), true)
			r.mu.Unlock()
			rec.Result = result
			r.emit(rec)
		})
	}
	return up, down, finish
}

// window 生成 f 从上一窗口结束到 now 的记录并推进窗口。调用方持有 r.mu。
func (r *Recorder) window(f *flow, now time.Time, final bool) Record {
	up, down := f.up.Load(), f.down.Load()
	rec := Record{
		ConnID: f.id, ConnStart: f.start.UnixMilli(), WindowStart: f.windowStart.UnixMilli(), WindowEnd: now.UnixMilli(),
		Final: final, Email: f.s.Email, Inbound: f.s.InboundTag, Network: f.s.Target.Network.SystemString(),
		Up: up - f.emittedUp, Down: down - f.emittedDown,
	}
	if f.s.Source.IsValid() {
		rec.Source = f.s.Source.NetAddr()
	}
	if f.s.Target.Address != nil && f.s.Target.Address.Family().IsDomain() {
		rec.Domain = f.s.Target.Address.Domain()
	}
	rec.Target = f.s.Target.NetAddr()
	if ob := f.s.Outbound; ob != nil {
		rec.Outbound = ob.Tag
		if d := ob.Dialed(); d != nil {
			if d.Local != nil {
				rec.Local = d.Local.String()
			}
			if d.Remote != nil {
				rec.Target = d.Remote.String()
			}
		}
	}
	f.windowStart, f.emittedUp, f.emittedDown = now, up, down
	return rec
}

func (r *Recorder) emit(rec Record) {
	line, err := json.Marshal(rec)
	if err != nil {
		return // 只含基本类型，不会失败
	}
	select {
	case r.queue <- append(line, '\n'):
	default:
		r.dropped.Add(1)
	}
}

// tick 每隔 interim 给已连满一个窗口的长连接补一条中间记录。
func (r *Recorder) tick() {
	defer r.done.Done()
	t := time.NewTicker(r.interim)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case now := <-t.C:
			var recs []Record
			r.mu.Lock()
			for f := range r.active {
				if now.Sub(f.windowStart) >= r.interim {
					recs = append(recs, r.window(f, now, false))
				}
			}
			r.mu.Unlock()
			for _, rec := range recs {
				r.emit(rec)
			}
		}
	}
}

func (r *Recorder) write() {
	defer r.done.Done()
	for {
		select {
		case line := <-r.queue:
			if err := r.out.Write(line); err != nil {
				errors.LogWarning(context.Background(), "flowlog 写文件失败: ", err)
			}
		case <-r.stop:
			for {
				select {
				case line := <-r.queue:
					_ = r.out.Write(line) // 退出前尽量写完；失败也只能放弃
				default:
					return
				}
			}
		}
	}
}

type countingReader struct {
	buf.Reader
	n *atomic.Uint64
}

func (c *countingReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := c.Reader.ReadMultiBuffer()
	c.n.Add(uint64(mb.Len()))
	return mb, err
}

func (c *countingReader) Interrupt() { common.Interrupt(c.Reader) }

type countingWriter struct {
	buf.Writer
	n *atomic.Uint64
}

func (c *countingWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	c.n.Add(uint64(mb.Len()))
	return c.Writer.WriteMultiBuffer(mb)
}

func (c *countingWriter) Close() error { return common.Close(c.Writer) }
func (c *countingWriter) Interrupt()   { common.Interrupt(c.Writer) }

func init() {
	capability.Register("flowlog")
	common.Must(common.RegisterConfig((*Config)(nil), func(_ context.Context, c interface{}) (interface{}, error) {
		return New(c.(*Config))
	}))
}
