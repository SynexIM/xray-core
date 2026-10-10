# 这个 fork 改了什么

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

每条规则持有一对桶，同规则命中的所有连接共同使用额度。由相邻档交点反推桶深度，满载流量依次降档；断开不会恢复满桶，闲置回满后才可清理。连接命中规则的交集条件必须全部成立；三个哈希索引取候选并集。多规则按当前限制从紧到松依次等待，零拥挤份额先等待。

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
| `app/accesslog/*`、`app/dispatcher/accesshook.go` | 访问日志聚合与 PullAccess，访问日志补齐目标 IP |
| `app/policy/*`、`features/policy/*`、`app/dispatcher/stats.go` | user_site 按站点计流量开关；本任务保持不动 |
| `app/reverse/*`、`app/reverse/command/*` | bridge/portal 增删查与并发保护；删 bridge 停 monitor，存量 worker 自然收敛 |
| `infra/conf/api.go`、`main/distro/all/all.go` | 新 app 与 command service 的注册 |
| `.github/workflows/*`、README、RELEASE | fork 自有发布、镜像与安全边界 |

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
