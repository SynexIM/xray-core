# 第 1 轮验证记录

受测提交 `c6df4f15`，Linux/arm64、Go 1.27.1、OrbStack 8C。基准命令见 `../../README.md`；完整改前/改后数据分别在 `../before/` 和本目录。

## 命令与真实结果

- `GOTOOLCHAIN=local ~/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.1.darwin-arm64/bin/go build ./...`：通过，无输出。
- `go vet ./app/... ./proxy/vmess/... ./testing/...`（同一工具链）：失败，3 条未修改文件中的告警：`app/router/command/command.go:232` 丢弃 cancel，`proxy/vmess/encoding/commands.go:33,67` unreachable code。未过滤后声称通过。
- `go test ./app/... ./proxy/... ./transport/...`：失败，`app/dns/TestQUICNameServer` 公网 QUIC DNS record not found；`transport/internet/tls/TestECHDial` Cloudflare ECHConfigList malformed；`transport/internet/finalmask/sudoku/TestSudokuE2ETemp/hysteria2/prefer_ascii` TCP reset（UDP c2s=10、s2c=0）。其余包通过。
- `go test ./app/limit/...`、认证/统计/dispatcher/proxyman/capability/protocol 受影响包：通过。
- 相关 `go test -race`：limit、stats/command、VMess 来源缓存/分段索引、并发增量导出均通过。
- 交叉编译 `app/limit` 测试并在 Linux 容器运行：通过，含真实 `SO_MAX_PACING_RATE`、byte/s 单位、<10% 不重设与无限速热改测试；稀疏小包 p99=27.84µs，满载字节 [208500,216000,220500]。
- 原 `testing/realnode/limit` 11 组真实 Xray 用例全通过：ladder/shared/outbound/hotswap/sourceips 各 upload、download，Vision download。双向阶梯均为 9.961→3.990→1.001Mbps；共享下载 8.014Mbps；多桶下载 19.998Mbps；热改下载 2.032→5.046Mbps；来源第三 IP 先拒绝、闲置 20 秒后放行；Vision 5.018Mbps，目的端 6262778B、统计 6288401B。
- `go test ./infra/conf -run 'TestEightProtocolIdentitiesReachOneLimitRule|TestRelayIdentityAndUpstreamUserShape|TestMissingEmailCannotMatchUsernameAsUser|TestLimit'`：通过（3.094s），八协议同桶、relay 身份与缺失 email 反例保留。
- HY2 mask 失败子例单独复测 `go test ./transport/internet/finalmask/sudoku -run 'TestSudokuE2ETemp/hysteria2/prefer_ascii' -count=1`：通过（5.066s）；整包运行失败记录仍保留，不改写为全量通过。

## 尚未达标与测量限制

- VMess 随机冷认证 p99 9060.793→4361.884µs，仍未达 200µs；最近/来源缓存 8.375/1.375µs 达到热命中目标。VLESS 百万身份 5.041µs。
- 新连接 426.313→424.873/s，EOF 22528→22213、节点 CPU 93.38→93.40%（8C）。吞吐不达标，失败比例高，不能把它当作无错误的生产容量；64 个测试源地址已排除单源临时端口耗尽。
- 固定到达 Attach p99 76.304305→1.185234ms，仍略高于 1ms；指标含请求调度时间，没有扣除 Go/VM 调度停顿。十万规则分页 30.352526→0.009000ms 达标。
- 满载小包 p99 51.095→42.711ms，改后未限速 p99=47.491ms；附加时延未超线，独立运行之间存在调度噪声，负差值不是物理负延迟。
- 6Mbps 恒定码率 UDP 37500 包全部送达；抖动 p99 13.293→8.900ms，未达 3ms。突发改前 0、改后 48 丢包；实际发送 12.795ms，也未满足严格 10ms 窗口。本轮没有修改第 2 轮的 UDP 队列与分层丢包计数，不能把该场景声称通过。
- 20M 浅缓冲中位吞吐 16.559→18.085Mbps，重传 0.076861→0%；吞吐未达 19–20M。tc 参照实测 7.340→8.214M，未达同速，不能完成同档参照验收。
- 300 下载合计 19.989Mbps，网页 p99 117.700ms；单连接 115.014ms，增加 2.686ms，达标。
- 路由单条增删 p99 22.99/22.40ms，留待下一轮；完整配置直启 6.423s，不代表尚未修改的 agent 交接。香港节点与真实专线未测。
- 原 realnode 速率语义通过不等于第 0 节全部性能达标；未修改或勾选 ProxyAstra 的施工清单。

## 上游挂钩增量

相对本轮起点 `4bdd199e`，仅计算 v26.10.10 已存在的 Go/proto 源文件，排除新增文件、测试与生成 pb：14 个，+216/−186 行。整个 fork 相对 v26.10.10 为 60 个，+2061/−435 行。

| 文件 | + | − |
|---|---:|---:|
| app/dispatcher/default.go | 58 | 33 |
| app/proxyman/command/command.go | 7 | 2 |
| app/proxyman/inbound/always.go | 6 | 0 |
| app/stats/command/command.go | 3 | 0 |
| app/stats/command/command.proto | 1 | 0 |
| app/stats/counter.go | 11 | 2 |
| app/stats/online_map.go | 11 | 0 |
| app/stats/stats.go | 46 | 78 |
| common/protocol/user.go | 5 | 3 |
| proxy/vless/inbound/inbound.go | 5 | 12 |
| proxy/vmess/aead/authid.go | 17 | 36 |
| proxy/vmess/encoding/server.go | 2 | 2 |
| proxy/vmess/inbound/inbound.go | 12 | 5 |
| proxy/vmess/validator.go | 32 | 13 |
