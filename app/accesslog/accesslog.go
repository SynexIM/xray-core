package accesslog

import (
	"context"

	"github.com/xtls/xray-core/app/accesslog/command"
	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/common"
)

// processAggregator 是进程级单例（节点 = 单 xray 进程）。
// app Init 时构造并注册 dispatcher hook；command 服务经注入的 puller 读它供 PullAccess。
// 与 fair scheduler 同心智的进程单例，避免引入一套 features/accesslog 注入机制。
var processAggregator *Aggregator

// Process 进程单例访问器。未配置 accesslog app 时为 nil。
func Process() *Aggregator { return processAggregator }

// App 是 accesslog feature app（仅用于通过 config 触发 Init 注册 hook + 起 Run）。
type App struct{}

func (*App) Type() interface{} { return (*App)(nil) }
func (*App) Start() error      { return nil }
func (*App) Close() error      { return nil }

func newApp(cfg *Config) *App {
	agg := New(int(cfg.GetChanSize()), int(cfg.GetMaxSitesPerEmail()))
	processAggregator = agg
	go agg.Run()
	// 把聚合器 Pull 注入 command 服务（command 不 import 本包，避免环依赖）。
	command.SetPuller(agg.Pull)
	// 注册 dispatcher 包级 hook：进程内嗅探事件直投聚合器有界 channel（非阻塞）。
	dispatcher.RegisterAccessHook(func(ev dispatcher.AccessEvent) {
		agg.Collect(Event{
			Email:    ev.Email,
			Domain:   ev.Domain,
			DestIP:   ev.DestIP,
			Protocol: ev.Protocol,
			UnixSec:  ev.UnixSec,
		})
	})
	return &App{}
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		return newApp(cfg.(*Config)), nil
	}))
}
