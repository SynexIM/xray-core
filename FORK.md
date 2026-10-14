# 这个 fork 改了什么

## 数据面第 2 轮：基准台归因（2026-10-15）

建连基准新增两侧 CPU/mutex/block profile（基准专用 profiled 入口加载正式二进制相同模块，不改变正式进程的采样设置）。节点 CPU 94.23% 的样本落在系统调用，91.58% 累计在出口 `syscall.Connect`；客户端仅 0.12 核。原台只分散客户端来源，节点出口仍对同一目标 IP:端口建连。现在目标也分散到 64 个地址，避免出口 TIME_WAIT 四元组耗尽。原 60 秒 424.873/s、22213 EOF、93.40% CPU；同一份节点代码在修正台上 60 秒 10893.644/s、0 失败、51.08% CPU（含锁/阻塞采样）。吞吐仍未达 2 万/s。原始 profile 与前后报告见 `testing/realnode/perf/results/after-round2/`；数字变化是基准台修正，不能计作数据面优化。

UDP 五处满队列计数统一为 `dataplane>>>udp_drop>>>{pipe,hub,dispatcher,wireguard,hysteria}`，通过现有 StatsService.GetStats/QueryStats 和 metrics debug/vars 导出。计数为进程生命周期累计值，包含该进程所有 Xray 实例；只能重置，不能注销。默认队列为 pipe 256KiB、hub 1024 包、dispatcher 256 包、WG/Hy2 各 1024 包；释放 dispatcher 接收的 Buffer，防止长期 UDP 读泄漏。pipe 写错误保留日志原因。五层强制溢出与 RPC 出口均有受影响包测试。

突发台改为先确认一次端到端 UDP 回显，再发 200×1200B/10ms，档位 250Mbps（该突发本身约 192Mbps，旧 100Mbps 档不能称“限速以内”）。改前 48 丢包/12.795ms；校正后 0 丢包/9.959ms。冷会话仍实测丢 101 包，五层、内核 UDP 错误、qdisc 丢包均为 0，缺失的是最前一批包；热身后归零，符合冷 ARP 未解析邻居队列溢出的特征，尚未用邻居 discard 计数确认，不能归因给 UDP 预借。2Mbps 原生 UDP 入站人为超速 600 包，丢 295＝pipe 219＋内核 RcvbufErrors 76。6Mbps/60 秒 37500 包全到，抖动 p99 9.420ms，仍未达 3ms。详细结果保留在 after-round2 的 udp-* 子目录。

基线：**XTLS/Xray-core v26.10.10**。本分支 `synexim/v26.10.10` 由 `752042bc` 将旧 fork 差异三方移植到该 tag。module 保持 `github.com/xtls/xray-core`，调用方通过 `replace` 使用。发布使用 SynexIM 自有通道，保留上游 MPL-2.0 与版权声明；发布约束见 [RELEASE.md](RELEASE.md)。

fork 改动集中在通用限速和运行时管理。本文记录当前差异；旧实现可在 Git 历史中查看，不保留并行兼容路径。

## 限速接口重塑（2026-10-10）

`protocol.User` 和 `MemoryUser` 回到 `{level,email,account}`，proto 旧字段 4–21 的号码与名字 reserved。删除旧 User 限速、pool/class、固定出口、每出站共享限速操作和 `app/fairshare`。出口选择由路由负责；出口上限由匹配 outboundTag 的限速规则负责。

## 管理服务（2026-10-10）

- `app/policy/command/`：PolicyService.SetLevels 整份替换 levels，GetLevels 返回有效值的独立副本；manager 用 atomic 指针发布不可变快照，新连接读新策略，已建连接保持原 Session。CLI：`xray api policy set|get`，set 接受含 `levels` 的 policy JSON 对象。
- `app/capability/`：模块自行 Register 能力，并发安全、去重、排序；CapabilityService.Get 与 `xray api capability` 返回版本和能力。构建发布版加 `-ldflags '-X github.com/xtls/xray-core/app/capability.Release=synexim.1'`，得到 `26.10.10-synexim.1`；不注入时只返回 core.Version()。本路登记 `policy.hotReload`、`api.batch`、`inbound.drain`，`limit` 由限速模块登记。
- `main/commands/base/execute.go`：命令后单独的 `--help` / `-h` 复用已有 Help 路径并成功退出，让分组命令与 CustomFlags 命令的帮助检查可用；不传帮助参数时保持原执行路径。

## 安全与部署

`infra/conf.DecodeStrict` / `DecodeStrictValue` 供控制面准入检查未知字段并返回路径；协议 config 自行声明 RawMessage 账户形状，路由严格校验复用原生 `rawFieldRule`，不改变命令行宽松解析。

SOCKS BIND：v26.10.10 上游已在 `proxy/socks/protocol.go` 拒绝 CMD=2，回复 RFC 1928 0x07；保留握手层实现，不加配置开关。`proxy/socks/bind_test.go` 覆盖认证/无认证、UDP 开关组合、0.0.0.0:0 占位，观察系统监听器确认没有开端口；UDP ASSOCIATE 不改。

部署示例：`deploy/examples/node-line.json` 的四协议 TCP 入站默认 `acceptProxyProtocol: true`，前面有中转必须开并要求中转发送 PROXY protocol；限制中转源地址，UDP 不经 PROXY protocol。`node-ip.json` 每 IP 一组 Mixed → freedom，明确 `sendThrough` 与 `ForceIPv4`。两例 API 只监听 `/run/xray/api.sock`，Compose host 网络的 xray/agent 共享 `/run/xray`，agent 镜像由下一个 change 提供。配置凭据均为占位，REALITY 私钥需运行 `xray x25519` 后填入；LimitService 等限速分支合并，示例暂不含 limit 段。

### 配置与执行器

| 路径 | 当前职责 |
|---|---|
| `app/limit/config.proto`、`infra/conf/limit.go` | node/profiles/rules 配置，阶梯与引用校验 |
| `app/limit/shaper.go` | 每规则上下行阶梯桶、稀疏流优先、满载流按字节公平 |
| `app/limit/limit.go` | 三索引规则匹配、连接/IP 准入、热更新与旧桶生命周期 |
| `app/limit/node.go` | 节点拥挤滞回、正常/重度加权注水 |
| `app/limit/link.go`、`features/limit/limit.go` | dispatcher 使用的小接口与传输包装 |
| `app/limit/command/`、`main/commands/all/api/limit.go` | LimitService 和运维 CLI |
| `testing/realnode/limit/` | 真实 Xray 子进程流量验收 |

JSON 的速率用带单位字符串、时间用 Go 时长；protobuf 用 bit/s 与毫秒。复用上游 Hysteria 带宽解析器，`1mbps = 1048576 bit/s`；精确十进制速率写 `1000000bps`。每条阶梯速率严格下降，只有末档不带 `for`。未配置方向/节点参数为零时不增加对应限制，不编造业务默认值。

每条规则持有一对桶，同规则命中的所有连接共同使用额度。由相邻档交点反推桶深度，满载流量依次降档；断开不会恢复满桶，闲置回满后才可清理。连接命中规则的交集条件必须全部成立；三个哈希索引取候选并集。多规则以一致的排队键协同等待，取最晚可放行时间一次等待，全部可用后一起扣；零拥挤份额仍暂停。

同名 profile 原地更新，存量连接下一片生效；同 ruleTag 规则替换只影响新连接，存量连接保持原桶。RemoveProfiles 拒绝删除仍被规则引用的 profile，返回 FAILED_PRECONDITION 并列出引用者。在线来源 IP 在最后活动后闲置 20 秒移出；多桶准入先一起校验再增加计数，失败不会留下部分占用。

只有配置 congested 的桶参与注水。节点利用率按每条 link 计量一次，扣除不参与桶的真实流量后再分配容量，避免多规则流量重复计算。利用率进入/退出阈值与 holdSeconds 共同控制拥挤状态。

### dispatcher、splice 与身份撤销

路由选定出站后，dispatcher 通过 feature 注册表取 Limiter 一次挂载：link 上行是 Reader、下行是 Writer。包装保留 UDP/XUDP 报文边界、目的地址及关闭/中断接口。未填 email 的账户保持空，不用用户名代填规则身份；HTTP/SOCKS 运行时账户必须提供显式用户名、口令。

命中限速的会话使用上下文共享原子标记禁用 Vision splice，避免零拷贝绕过桶。加载 limit 后，未命中的 link 也挂不排队的计量包装并禁用 splice，确保节点用量完整以及 SetNode 热开对旧连接的计量有效；代价是失去这些连接的 splice 吞吐。完全不加载 limit 时保留上游路径。

会话表按 email 登记，弱身份键单独处理删除与认证的竞态，不给 MemoryUser 增加私有运行字段。删除账户撤销旧身份并中断现有传输；KillUserConnections 只断已有连接，同凭据仍可重连。mux 子流共享禁用 splice 标记。

### 管理接口

启用方式为 `api.services: ["LimitService"]`。提供 SetNode、PutProfiles、RemoveProfiles、AddRules、RemoveRules、ListRules、GetStatus。分页列表仅返回 ruleTag/profile；状态返回节点拥挤、利用率，按 ruleTag 返回各方向档位、桶余量/容量、连接数与在线 IP 数。

`HandlerService.KillUserConnections(emails)` 不删除身份。CLI 提供 `xray api limit setnode|putprofiles|rmprofiles|addrules|rmrules|lsrules|status` 和 `xray api killconn`，从 JSON 文件读取参数。能力登记由另一任务处理，本任务不添加 capability 模块。

### 验证与限制

- 八协议配置与身份映射矩阵、SS2022 relay、UDP/XUDP 边界、共享桶、多桶、热改、稀疏时延、拥挤注水及零份额反例均有测试。
- 小包基线 p50=0.542µs、p99=1.333µs；满载背景下整形 p50=5.375µs、p99=50.708µs，满足附加时延门限。
- 真进程 60 秒阶梯 [10Mbps 5s, 4Mbps 20s, 1Mbps] 实测为 9.961→4.005→1.001Mbps；不限速上传基线 16920.16Mbps。测试窗口与阈值见工具 README。
- 四账户共享规则 8.014Mbps；五账户桶叠加出口规则 19.998Mbps；存量上传热改 2.032→4.981Mbps，下一秒生效。Linux 来源 IP 第三个地址先拒绝，断会话并闲置超过 20 秒后放行；Vision 4.981Mbps，目的端 6262778 字节与统计 6289328 字节偏差约 0.42%。
- 真实 gRPC 验证七个 RPC、错误码、分页；真实 SOCKS 流按 email 断开约 1.24ms，同凭据重连成功，删除后认证失败。
- 全量 race 门禁存在上游 `proxy/blackhole/TestBlackholeCustomResponse` 测试自身竞争（第 66 行写，第 71/73 行读）；未修改无关测试。过滤指定上游告警后的 vet 无输出。
- 八协议矩阵不等于八协议公网实测。HY2 Brutal 与整形器叠加、真实公网节点及大规模混合协议负载仍待节点验收。

## 保留的管理能力与其他差异

| 路径 | 差异 |
|---|---|
| `app/proxyman/command/*`、`proxy/proxy.go` | 凭据 UpdateUser；批量 AddUsers/RemoveUsers/BatchAlterInbound；DrainInbound/ResumeInbound；指定 tag 列表与 email 查询 |
| `proxy/http/users.go`、`proxy/http/*`、`proxy/socks/*` | 显式凭据的运行时账户管理、按 email 管理；本任务不改 SOCKS BIND |
| `common/protocol/user_sessions.go`、各协议删除路径 | 撤销认证身份与按 email 中断 TCP/UDP 会话 |
| `proxy/shadowsocks/validator.go`、`proxy/vmess/validator.go` | email 索引与批量管理，减少线性扫描 |
| `infra/conf/shadowsocks.go` | 显式空 clients 保留 SS2022 多用户 handler，支持运行时添加首位客户；SS2022 内核使用上游重写版 |
| `app/proxyman/outbound/outbound.go` | 读锁内读取 tagsCache，避免增删出站竞争 |
| `app/router/*`、`app/router/command/*` | BatchAddRule/BatchRemoveRule/ListRuleFull，保留原生规则快照，命名规则原子替换；规则列表保留 user/inboundTag |
| `app/stats/command/*`、`features/stats/*` | 指定 email 查询在线状态与累计流量，不重置其他身份 |
| `app/flowlog/*`、`features/flowlog/*`、`infra/conf/flowlog.go` | 每连接一条审计记录（JSON 行、按大小轮转、60 秒中间记录、有界不阻塞），取代已删除的 `app/accesslog` 聚合与访问钩子 |
| `app/proxyman/outbound/handler.go`、`common/session/session.go` | 出站拨通后把本机 / 对端地址存进 `session.Outbound`（原子），供审计回答"出站本机 IP:端口" |
| `app/policy/*`、`features/policy/*`、`app/dispatcher/stats.go` | user_site 按站点计流量开关；本任务保持不动 |
| `app/reverse/*`、`app/reverse/command/*` | bridge/portal 增删查与并发保护；删 bridge 停 monitor，存量 worker 自然收敛 |
| `infra/conf/api.go`、`main/distro/all/all.go` | 新 app 与 command service 的注册 |
| `.github/workflows/*`、README、RELEASE | fork 自有发布、镜像与安全边界 |

## 节点规模与 TCP 模板（2026-10-11）

- `infra/conf/transport_fingerprint.go` 将 `sockopt.tcpFingerprint` 的 none/windows/macos/android 编为原生 SO_MARK、TCP_WINDOW_CLAMP、必要的 SO_RCVBUF；普通 mark 不变，冲突参数拒绝。TCP 字节转换由独立 agent 的 tc 程序负责，不扩展 socket proto。模板不能代表完整操作系统行为。
- `app/router` 的批量删除与命名替换只发布一次不可变快照；失败的替换不发布、旧规则保留位置，新规则按请求顺序追加。单条修改依然 O(N)。1000 条删除、表长 100000 的本机基准从 7004.053ms 到 9.5839ms。
- `UpdateInboundOperation` 仅原地修改嗅探策略，worker 原子读取；监听器、代理配置和账户变化拒绝。能力 `inbound.hotReload`，真实 SOCKS 用户与长连接测试通过。
- SS2022 上游已有 O(1) 身份与 email 索引，本阶段不再改它；百万账户基准只验证现有结构。
- 健康基线：相对 v26.10.10，修改原有上游 Go/proto 文件 53 个，+1780/−246 行；不计新增文件、测试、生成 pb 与资产。目标是逐步减少上游挂钩，而非把新目录代码计为上游改动。

## REALITY 与上游更新

直接使用上游 REALITY，无闭源依赖。上游自 v26.9.9 所用版本已将记录缓冲区扩大到 `17 * 1024`，旧 SynexIM replace 已移除。

跟随上游时，将相对旧 tag 的整份差异三方应用到新 tag，解决实际冲突；改动 proto 重新生成，运行构建、相关 race 和真实节点工具。不得把本地性能结果当作公网节点验收。

```bash
git checkout -b feat/<新tag>-synexim <新tag>
git diff --binary <旧tag> <fork分支> | git apply -3
go run ./infra/vprotogen -pwd .
go build ./...
go test -race ./app/limit/... ./app/dispatcher/... ./app/proxyman/... ./infra/conf/... ./common/protocol/... ./proxy/...
```

上游 router command 的超时 cancel 告警、协议 unsafe.Pointer 等指定 vet 告警保持原状。geoip/geosite 及部分网络测试需要外部资源，不能只凭构建声称通过。
- `app/proxyman/inbound`：补齐 `Manager.DrainHandler` / `ResumeHandler` 与 AlwaysOn / worker 的排空实现（rebase 时漏移植，从 20bc7afb 找回）。`DrainInbound` 只关监听口、已建立会话继续；gRPC / H3 等关监听会断流的传输拒绝排空。ProxyAstra agent 新旧交接依赖它。

## 数据面性能基准（2026-10-14，第 1 轮）

`testing/realnode/perf` 使用现有 validator、配置入口、SOCKS 与 realnode 启动工具，没有新增 Go 依赖。`limit/main.go` 的启动器迁到 `testing/realnode/internal/runner`；真实核心二进制在改前测量期间保持 `4bdd199e`。结果、命令、环境与限制见工具 README 和 `results/before/baseline.{json,md}`。

OrbStack Linux/arm64、Go 1.27.1、8C，client/node/target 命名空间与透明瓶颈桥隔离运行；模拟 30ms RTT、1Gbps 链路和 21Mbps/20 包浅缓冲。VMess 2 万身份随机/最近/来源场景 p99 为 9060.793/8116.780/7323.186µs；VLESS 百万身份 2.042µs。百万桶两万活跃的固定到达 Attach p99 为 76.304305ms，十万规则一页 p99 为 30.352526ms。百万身份 heap 增量 824.840B/身份，删除后仍有 200 万计数器。

这些是改前数据，尚未声称达标：2048 并发新连接 426.313/s、22528 次 EOF；恒定码率 UDP 无丢包、抖动 p99 13.293365ms。tc 参照实际中位吞吐只有 7.340Mbps，不能当成 20Mbps 的同速对照；尝试独立桥与 fq 后仍未达标，保留实测并列为验收限制。香港生产节点未连接，真实专线按施工单暂缓。完整配置就绪不等于 agent 的逐条 gRPC 交接时间。

### 认证与统计

VMess 在来源 IP 上缓存弱解码器引用，先试缓存，未命中按 64 段最近成功顺序试解；缓存不绕过时钟与重放校验，删除身份使缓存失效。来源缓存最多 65536 项，避免一次性来源无限占用内存。`vmess.maxUsersPerInbound=20000` 经 CapabilityService 的 `limits` 字段上报，单条和整批 VMess 写入均守上限；冷认证仍与身份数相关，上限不是冷认证延迟达标承诺。validator 暴露认证直方图，入站增加 pprof 标签。

用户统计在装载时建立句柄；普通计数器 64 片读写锁，用户累计值按 email 保存一次。删除按装载引用回收上下行、连接 gauge、在线表和显式开启的站点计数器；同名身份仍被其他入站使用时保留。VLESS 旧的无条件注销路径已删除。`GetUsersStats.onlyChanged` 只消费变化集合，包含离线流量，指定 email 不消费其他身份。ProxyAstra agent 尚未切换。

`results/after-auth-stats/` 实测：来源缓存 p99 7323.186→0.750µs，最近身份 8116.780→1.666µs，均匀冷身份 9060.793→4832.715µs（未达 200µs）；百万身份 heap 824.840→778.724B/身份，删除后计数器 2000000→0。相关包测试通过，含缓存撤销/重放、容量整批拒绝、同名引用、并发增量清零、站点回收和已有句柄零分配。

### 限速运行时与排队

规则写入一次发布不可变索引与有序分页快照；Attach 不获取管理表锁，准入仍按桶互斥保证多桶原子性。首次 Attach 登记活跃集，末条连接结束且额度回满后退出；巡检只访问活跃集，退役队列单独回收，正常桶的来源 IP 惰性过期。存量桶与 profile 热改语义不变。

连接预建排队身份并缓存速率版本；等待复用 waiter/channel，限速等待零分配。多桶使用同一稀疏级别、虚拟字节标签与全局序号，避免不同桶队头互相等待；全部桶一起放行记账。小包走借量子快路径，UDP 保持整包并最多欠一个量子；超大量报文按片支付后整包发送。亚毫秒等待唤醒队头执行 Gosched 再查，大等待使用定时器。

Linux 在客户独占 TCP socket 上设置 `SO_MAX_PACING_RATE`（byte/s），下载满载连接按活跃满载份额分配，稀疏连接优先，变化小于 10% 不重设。热改成无限速清除旧上限，连接结束不复位。复用已有加密/统计/TLS/REALITY/PROXY protocol 解包工具；不暴露独占 fd 的多路复用传输与 UDP 保持应用层整形，其他系统为空实现。

`results/after-runtime/` 实测：百万桶两万活跃、固定到达 Attach p99 76.304305→1.368411ms（仍高于 1ms）；十万规则分页 p99 30.352526→0.032334ms。现有 app/limit 测试与新增零分配、多桶无死锁、借额度界限、管理锁隔离、Linux socket 单位/10% 阈值/无限速热改测试通过；相关并发 race 通过。真实节点原用例与整轮改后基准在收尾验证，不能用单测替代。

### 第 1 轮收尾结果

整轮基准见 `testing/realnode/perf/results/after-round1/baseline.{json,md}`，验证、上游文件明细与未达标原因见同目录 `validation.md`。百万桶 Attach p99 76.304→1.185ms、十万规则分页 30.353→0.009ms、百万身份 heap 824.840→778.582B；删除后计数器 200 万→0。300 下载 19.989Mbps，网页 p99 仅增加 2.686ms；浅缓冲重传 0.076861→0%，中位吞吐 16.559→18.085Mbps。恒定 UDP 无丢包，抖动 p99 13.293→8.900ms；突发 48 丢包。新连接 426.313→424.873/s、CPU 93.40%、大量 EOF，仍未达标，不能声明生产容量。

原 realnode 限速 11 组全部通过，速率、共享、多桶、热改、来源 IP、Vision 统计语义不回退；八协议配置/身份矩阵通过。构建与相关 race 通过；要求的未过滤 vet 有 3 条既有告警，全量目标包测试有公网 QUIC DNS、Cloudflare ECH 和 HY2 mask 3 处失败，HY2 mask 失败子例单独复测通过，未改写整包失败结论。冷认证、巡检 1ms 线、UDP 抖动/突发、浅缓冲吞吐和同速 tc 对照仍未通过；香港与真实专线未测。本轮上游源文件增量 14 个 +216/−186，整个 fork 为 60 个 +2061/−435（排除新增、测试、生成物）。没有把施工清单等同于验收通过。
