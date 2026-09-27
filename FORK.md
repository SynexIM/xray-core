# 这个 fork 改了什么

基线：**XTLS/Xray-core v26.9.9**（2026-09）。此前基线 v26.7.28；fork 与上游没有共同提交历史，
所以换基线的做法是把 `git diff v26.7.28 <fork 分支>` 三方合并到新 tag 上（见文末「怎么跟上游」）。

module 名保持 `github.com/xtls/xray-core` 不变——3x-ui 通过 `go.mod` 的 `replace`
指过来，改名反而要动上游的每一处 import。

**原则：patch 尽量薄，且集中在限速与运维接口这两块，不碰路由与协议本身。**
这样 rebase 上游永远是几个文件的事。

## 新增（上游完全没有的东西）

```
app/fairshare/              节点级公平限速 + command service
app/accesslog/              访问日志聚合 + command service（PullAccess）
app/reverse/command/        reverse（bridge/portal）热改的 gRPC 面
app/dispatcher/accesshook.go
common/buf/limit.go         每用户带宽整形
common/protocol/user_limits.go     每用户限速状态
common/protocol/user_conns.go      每用户连接数
common/protocol/tier_shaper.go     按池整形（标准/突发）+ 按连接公平低时延整形，节点上唯一的限速执行器
common/protocol/node_fairshare.go  节点级拥塞门控：只在拥挤时按 class 注水改写各池速率
infra/conf/user_runtime.go         各协议客户端 JSON 共用的方向/池整形/egress_tag/pool 字段
app/proxyman/outbound/rate_limit.go  每出站共享总带宽桶
proxy/http/users.go         给 http 协议补上客户端（email）管理
```

## 改动（在上游文件里加东西）

| 文件 | 加了什么 |
|---|---|
| `common/protocol/user.proto` | 顶层限速、连接数、`committed_bps` / `committed_burst_bytes`、`class`；另收敛 Nodus/IPNex 的方向限速字段与 `egress_tag`（字段 9–15） |
| `common/protocol/user.go` | `ToMemoryUser` / `ToProtoUser` 保留全部运行态字段并容忍无 account 的用户 |
| `common/protocol/user_limits.go` | 单一 per-user shaping seam：无方向字段仍走现有共享 PIR/CIR/CBS；有字段时上传/下载桶隔离 |
| `app/dispatcher/default.go` | 限速挂到 link；连接数上限与 active gauges；用户固定出口；按站点计流量 |
| `app/proxyman/outbound/outbound.go` | `Select` 在读锁内读 tagsCache（上游并发增删出站时的数据竞争） |
| `app/proxyman/config.proto` | `SenderConfig.rate_limit_bit_per_sec`，每出站上下行合计共享的总速率 |
| `app/proxyman/command/*` | `DrainInbound` `ResumeInbound` `BatchAlterInbound`；`AddUsersOperation` `RemoveUsersOperation`；卸载时清运行态；`SetOutboundRateLimitOperation` 热改出站总速率 |
| `app/router/command/*` | `BatchAddRule` `BatchRemoveRule` `ListRuleFull` |
| `app/router/router.go` | ListRule 带回规则自身的 user/inboundTag（`Route.GetUser` 不再对 nil Context 崩溃）；热更新并发安全由上游 v26.9 的原子规则表提供 |
| `app/reverse/*` | Reverse 加锁 + 存 dispatcher/ohm，开放 bridge/portal 的增删查 |
| `app/policy/*` | `user_site` 开关（按站点计流量，默认关——域名基数无上限） |
| `common/session/session.go` | `DialedRemoteAddr`（访问日志补齐目标 IP 用） |
| `features/policy/policy.go` | `Stats.UserSite` |
| `proxy/proxy.go` | `UserUpdater` 接口；`BatchUserManager` 接口；受限用户强制走 buffered copy |
| `proxy/http/*` | 客户端管理与限速 |
| `proxy/vless/inbound/inbound.go` | 删用户时重置限速器与连接数 |
| `proxy/socks/config.proto` `proxy/http/config.proto` | `UserAccount` 也带双速率字段与 `class` |
| `infra/conf/shadowsocks.go` | 显式 `clients: []` 保留 SS2022 空多用户 handler，首位客户可走运行时热添加 |
| `testing/scenarios/socks_test.go` | 真进程热添加 SOCKS 用户后按墙钟验证带宽上限，覆盖 Linux splice 路径 |
| `proxy/shadowsocks_2022/config.proto` | `RelayDestination` 带四个限速字段与 `class`（relay 没有 User 消息） |
| `proxy/shadowsocks_2022/inbound_multi.go` | email→下标索引；`AddUsers`/`RemoveUsers` 批量入口 |
| `proxy/shadowsocks/validator.go` `proxy/vmess/validator.go` | email→下标索引，`Del`/`Remove` 从 O(N) 变 O(1) |
| `proxy/shadowsocks_2022/inbound_relay.go` | 每个 destination 一个长期 `MemoryUser`；顺带补上漏传的 `level` |
| `proxy/http/users.go` | `UserStore` 把双速率翻进 `MemoryUser` |
| `infra/conf/*.go` | 各协议解析 `committed_bps` `committed_burst_bytes` `class` |
| `infra/conf/api.go` | 注册三个新 command service |
| `main/distro/all/all.go` | 引入新 app 与 command service |

Nodus/IPNex 曾在删除的 vendored snapshot 中维护方向限速、固定出口和 active
connection gauges。本 fork 将其经过测试的运行语义收敛到这份 canonical tree：不再
保留第二份 xray 权威。

### 为什么限速字段放在 `User` 顶层而不是各协议的 account 里

放顶层意味着**每个协议自动都有**限速——vless、vmess、trojan、shadowsocks、
mixed 全都通过同一个 `ToMemoryUser` 拿到，不需要每个协议各自实现一个
`RuntimeLimits()` 方法。少一个协议实现，就少一个"这个协议的限速没生效"的坑。

副作用是好的：`protocol.User` 的 protobuf json tag 就是 `bandwidth_bps`，
所以凡是直接 `json.Unmarshal` 到 `protocol.User` 的协议（vless、vmess），
配置解析**自动就通了**，一行代码都不用加。后来加的 `committed_bps` /
`committed_burst_bytes` 同样白拿这个好处——不过这一点不靠假设，
`infra/conf/limits_matrix_test.go` 逐协议实际验证过。

### 为什么每用户限速字段暂时不能退出核心 `User`

本轮新增的**出站级**上限没有继续污染 `User`：它在
`proxyman.SenderConfig.rate_limit_bit_per_sec`。该字段是 optional：未出现的普通
出站保持上游 splice 快路径；显式出现（即使值为 0）的托管出站才进入可热改模式，
运行时一个 outbound Handler 持有一个稳定令牌桶，所有客户、上下行共同经过它。
托管出站固定走 buffered copy，避免 Linux splice 绕过桶。`AlterOutbound` 的
`SetOutboundRateLimitOperation` 原地改桶，不换对象，因此既有连接不重启也能生效。

但这不能替代 `User` 上现有的每客户 PIR/CIR/CBS。两者管的是不同边界：

- 出站桶守住一个共享上游出口的总成本；
- User 桶守住单个客户自己的速率与突发，不让一人吃掉整个共享出口。

当前 Xray 没有独立于协议 User 的通用 per-client policy seam。若现在硬搬，启动配置、
`UpdateUserOperation`、dispatcher 取运行限额、各协议的身份映射，以及没有 `User`
消息的 SS2022 relay 都要各自再造一条映射；漏任何一条都会出现“面板显示限速，
该协议实际不限”的静默失败。`infra/conf/limits_matrix_test.go` 证明现有顶层字段
至少完整穿过了这些真实协议边界。

所以本轮明确接受它们暂留核心类型。退出条件不是“找到更好看的 proto”，而是同时具备：

1. 一个上游可接受或 fork 内稳定的通用 per-client limiter app，拥有启动时声明配置与
   gRPC 热更新；
2. dispatcher 只通过稳定客户身份查询该 app，不再从 `MemoryUser` 读取限额；
3. 全协议配置矩阵、SS2022 relay、既有连接热改与真流量测试全部迁到新 seam；
4. 3x-ui 与 API 调用方已经改用新契约，并完成一个同版发布窗口。

四项满足后，删除 `User` 的 PIR/CIR/CBS 字段及协议复制字段；在此之前不建双写兼容层。

### ss2022 relay 是唯一一个"限速字段放顶层"占不到便宜的地方

relay 模式的配置里根本没有 `User` 消息：每个 `RelayDestination` 自带一个 PSK，
**它本身就是一个用户**。于是顶层字段的好处在这里失效——走 relay 的客户
设任何限速都会被静默丢掉，面板显示限住了，节点上一个字节都没限。

所以 `RelayDestination` 自己带上那四个字段（名字与 `protocol.User` 一致），
`RelayDestination.ToMemoryUser()` 把它翻成 `MemoryUser`，dispatcher 那边就
一视同仁了。这个方法是导出的，因为 `infra/conf` 的全协议限速矩阵要拿**同一段
映射**做断言——两边共用一个函数，relay 漏搬字段矩阵就会红。

还有一个不改就白改的地方：原来 `NewConnection` / `NewPacketConnection`
每来一条连接就 `&protocol.MemoryUser{...}` 现造一个。限速桶是按 `*MemoryUser`
指针缓存的，现造 = 每条连接一套满桶 = 开 N 条连接就是 N 倍速率。
现在跟 `MultiUserInbound` 一样，构造时就把 `users[i]` 建好，全程复用。

### 双速率：PIR / CIR / CBS

专线卖的是「承诺速率 + 允许突发」，一个桶表达不了：按峰值卖成本兜不住，
按承诺卖客户觉得慢。所以限速器是**一串桶**，流量依次通过每一个：

```
bandwidth_bps          PIR  峰值速率，突发能到多快        bit/s，0 = 不限
committed_bps          CIR  承诺速率，长期稳定给多少      bit/s，0 = 不设
committed_burst_bytes  CBS  能以峰值速率花掉多少额度      字节，0 = 没有
```

`committed_bps = 0` 时只有峰值桶，单速率行为**一个字节都没变**。
设了 CIR（且 CIR < PIR）就在峰值桶后面串一个更深的承诺桶：新连接上来时
承诺桶是满的，立刻放行，只有峰值桶在排队 → 跑 PIR；CBS 花完后承诺桶开始
按 CIR 滴令牌 → 自然落到 CIR。不需要任何额外状态机。

**CBS = 0 就是没有额度**：承诺桶只有默认小窗口，等于单速率 CIR。额度由控制面给，内核不替它编一个。

两个容易配错的边界，处理原则是**配错的后果应该是限住，不是放开**：

- `CIR >= PIR`：串上去只是白多一次 WaitN，忽略，退化成单速率。
- `PIR = 0` 而 CIR 已设：当单速率 CIR 处理，**CBS 忽略**。CBS 的定义是
  「能以峰值速率花掉多少」，没有峰值速率时它无处可花；照搬 CBS 当 burst
  会让只填了 CIR 的用户先白拿几十 GB 不限速额度。

双速率有三层测试，缺一层就会留下一段「只靠编译保证」的空白：

| 层 | 文件 | 证明什么 |
|---|---|---|
| 配置 | `infra/conf/limits_matrix_test.go` | 每个协议都真的解析出三个字段 |
| 桶 | `common/protocol/dual_rate_test.go` | 桶串对了（虚拟时钟，无 sleep） |
| link | `app/dispatcher/dual_rate_link_test.go` | dispatcher 真的把桶挂到了 link 上，**且四个方向都挂了** |

link 层是真推字节量速率的：突发段应在 PIR 附近，CBS 烧干后稳态段应落在 CIR 附近。
四个方向分别测——`getLink` 的上行/下行、`WrapLink` 的 Reader/Writer——因为
`getLink` 的两条独立管道方向极易搞错（历史上上行漏过限速），只测一个方向
另一个方向漏了不会有任何提示。

### 池整形 + 拥塞门控：不挤不限到持续，挤了才按 class 分

节点上只有一个限速执行器：`tier_shaper.go` 的池整形器（一个池一对上下行）。
节点调度器 `node_fairshare.go` 不再挂自己的桶，只在拥挤时改写池整形器的速率。

```
            不挤（used < enter%·root_cap，或没开 root_cap）      挤
池整形器    标准封顶；有额度跑突发                               速率 = 调度器给的份额
额度        按放行字节扣、按标准回补（= 只扣超出标准的部分）    同左（份额 ≤ 标准，额度只会回补）
持续速率    不起作用                                             重度池的保底
```

- **池**：`User.pool` 显式指定（控制面下发 = 实例 id）。同池的所有用户对象——多个入站、多个协议——
  共用一份速率与额度，绝不 ×N。pool 为空时以 email 为池。
- **拥挤时的注水**（每秒一次，work-conserving）：class 聚合保底 → 正常池地板 → 重度池保底（= 持续速率），
  每层整层给得起才发；剩余按 weight 先注给正常池（目标到标准），再注给重度池。谁也没被压住就全部放回标准。
- **重度池**：class 的 `heavy_window_seconds` 内平均用量 ≥ 标准 × `heavy_percent`%。窗口前的时间按零算，
  刚跑满的人不会立刻被当成重度。任一为 0 = 不识别。
- **时延**：稀疏流优先——连接近期用量低于池内公平份额即不排队（池令牌可欠账 25ms 速率，由满载连接还）；满载连接按 5ms 片 SFQ 轮转；公平按内层流（mux/XUDP/HY2 每条流各一份）。拥挤时也一样（份额只改速率）；未排队的池份额抬到同层水位。门禁：`common/protocol/tier_latency_test.go`（小包 p50 ≤ 1ms、p99 ≤ 3ms）。
- **限速包装**：`buf.RateLimitReader/Writer` 把 Interrupt/Close/ReturnAnError/Recover 交给里面的管道，mux/XUDP 会话照常关闭。
- 所有参数 0 = 没有这一项；class 表里没有的名字 = 不加权、无地板。内核里没有业务名字，也没有默认值。

测试：`common/protocol/pool_shaping_test.go`（不挤时突发→标准且永不降持续、pool 显式共享、拥挤时权重/地板/重度）。

### ⚠️ 单位陷阱：同一个 `_bps` 后缀，两处含义差 8 倍

```
common/protocol/user.proto        bandwidth_bps / committed_bps   比特/秒
app/fairshare/command/*.proto     avail_bps / *_byte_per_sec       字节/秒
app/proxyman/config.proto         rate_limit_bit_per_sec           比特/秒
app/proxyman/command/command.proto rate_limit_bit_per_sec          比特/秒
```

前者是业务单位（面板按 Mbps 展示后 ×1e6），后者是限速器单位。唯一的换算点是
`user_limits.go` 的 `bitsPerSecondToRuntimeBytesPerSecond`。

这两组历史字段**不改名**（改名会断掉已经在跑的 node-agent），改为在两份 proto
里各自写死语义，并由 `app/fairshare/command/command_units_test.go` 钉住。
谁要是「顺手统一成同一单位」，那两组测试会红——而线上的症状会是
「节点被掐到 1/8 速度」，从现象倒查回来要几天。

**新增的速率字段一律带 `_bit_per_sec` / `_byte_per_sec` 后缀，不许再用裸 `_bps`。**

### 不许有默认值

```
每客户端 bandwidth_bps / committed_bps / 标准   0 → 不套桶
committed_burst_bytes                            0 → 没有额度
节点 avail_bps                                   0 → 节点级调度关闭
class 不配 / floor / reserved / heavy_*          0 → 没有这一项
```

0 就是「没有」，不是「用默认值」。开了节点调度又不配地板，极端拥挤时池可能被压到接近 0——
这必须是控制面明知的选择，不能由内核替它默默决定。

### 5 万实例下的客户换手：批量入口与 email 索引

目标是 5 万实例/单节点组，而客户换手（到期释放、续费、改配）天天在发生。
原来四处管理路径都随用户数线性增长，其中最贵的是 **SS2022 的 EIH 表整份重建**
（`sing-shadowsocks` 的 `MultiService` 只暴露 `UpdateUsers`，`uPSK`/`uPSKHash`/
`uCipher` 三张表都是包内私有，没有增量入口；上游自己在 `inbound_multi.go` 里
留了注释承认这里性能不行）。逐个增删 5000 个客户 = 重建 5000 次全表。

两件事一起做：

- **email→下标索引**（ss2022 / shadowsocks / vmess 三处）：`Del`/`Remove`/
  `GetUser` 从 O(N) 变 O(1)。
- **批量入口** `AddUsers`/`RemoveUsers`（`proxy.BatchUserManager`，可选能力）：
  一批只重建一次表、只锁一次，且整批原子——批里有一个坏的整批不生效，
  不留会让 configHash 漂移的半截状态。命令面对应
  `AddUsersOperation`/`RemoveUsersOperation`；没有批量能力的 proxy 自动退回逐个。
  （原有的 `BatchAlterInbound` 只是把 N 个 RPC 合成一个 RPC，落到 inbound 上
  仍然是 N 次单客户操作。）

实测（`proxy/shadowsocks_2022/inbound_multi_churn_test.go`，5 万用户底数、
增删各 5000 个）：

| | 耗时 | 认证路径累计被锁 | 最长一次 |
|---|---|---|---|
| 逐个 | 4 分 56.8 秒 | 4 分 56.4 秒 | 560 ms |
| 批量 | 60.8 ms | 60.8 ms | 31 ms |

**热路径一个字节都没动**：VLESS/Trojan/VMess/SS2022 的认证本来就是 O(1)；
旧版 SS AEAD 的逐个试解是**协议缺陷不是代码缺陷**，靠「SS 一律用 2022 版」规避。

顺带修了一个安静的泄漏：per-user 限速桶挂在全局 `runtimeLimiters`（`sync.Map`，
键是 `*MemoryUser` 指针），用户被删掉之后没人清，那张表继续攥着指针——
既回收不了内存，也让这个用户永远活在进程里。5 万实例的月度换手会稳定攒出几万个
僵尸条目。清理放在命令层**知道用户是谁**的那一处（先查后删再清），
五个协议共用一份逻辑，不会有哪个漏掉。

### ⏳ 还没验证的：Hysteria2 的 Brutal 与整形器叠加

Hysteria2 的 Brutal 拥塞控制**主动忽略拥塞信号硬发**。它与用户态整形器叠加时
会不会打架，只能在真节点上实测，不是看代码能得出的结论。**结论出来之前，
不要假设 hysteria 线路的限速行为与其他协议一致。**

### 为什么 reverse 要能热改

API 调用方给连接换入口是常规操作。配置只在启动时读一次的话，换一个连接的入口
就得重启 xray——那台节点上**所有**客户的连接会一起断。一个人改配置、
全节点陪着断线，这不是优化问题。所以有了 `app/reverse/command`。

删 bridge 时只停 monitor（不再建新 worker），不强杀存量 worker：它们各自带
60 秒无活动回收，强杀反而要和 monitor 周期任务抢 `b.workers`。
让存量连接自然收敛，本来就是热改想要的效果。

### 为什么公平限速开启时全节点放弃 splice

Linux 的 splice 在两个 socket 之间零拷贝直通，会绕过 dispatcher 挂在 link 上的
限速包装器。没有 per-user 限速的用户既逃掉公平整形，也不进活跃字节统计
（不占公平份额的分母），拥挤时会挤压守规矩的用户。

判「这个用户受不受限」要用 `MemoryUser.HasRuntimeLimits()`，它覆盖所有限速字段。
只看 `bandwidth_bps` 会漏掉只配了承诺速率（CIR）的用户——他的 `bandwidth_bps`
是 0，会被判成不受限而走 splice，限速配了却一个字节都限不住。

所以公平开启 = 全节点 buffered copy。代价是失去 splice 的极限吞吐。
这是产品决策：**公平 > 极限吞吐**。公平没启用时 splice 照旧。

## 依赖替换：REALITY（v26.9.9 起已删除）

原先 `go.mod` 把 `github.com/xtls/reality` 换成 `github.com/SynexIM/reality`，只为把读借用目标
记录的缓冲区从 8192 放大到协议上限 16645（RFC 8446 §5.2）。上游 REALITY
`v0.0.0-20260908062103` 起已把它改成 `17 * 1024`（≥ 16645），v26.9.9 依赖的版本已含此修复，
所以 replace 删掉，直接用上游。

## 有意不改的

- `app/router/command/command.go:170` 的 context 泄漏（`context.WithTimeout`
  的 cancel 被丢弃）。这是**上游自带**的，v26.7.28 与 v26.9.9 都一样。
  改它只会增加 rebase 的冲突面，而泄漏 4 秒后自己就释放了。
- `app/stats` 与 `features/stats`：上游已经把 `GetOrRegisterCounter` 提升成
  Manager 接口的方法，比 fork 原来的包级辅助函数更好，所以**用上游的**。

## 怎么跟上游

```bash
git remote add upstream https://github.com/XTLS/Xray-core.git
git fetch upstream --tags
# fork 与上游无共同祖先：把相对旧基线的整份差异三方合并到新 tag 上，再逐个解冲突
git checkout -b feat/<新 tag>-synexim <新 tag>
git diff --binary <旧 tag> <fork 分支> | git apply -3
# 改过的 .proto 一律重新生成（protoc + protoc-gen-go/-go-grpc），go mod tidy，跑变更包 -race
```

冲突只会出现在上面那张表列出的文件里。改完跑：

```bash
go run ./infra/vprotogen -pwd .   # proto 变了才需要
go build ./... && go test ./app/... ./common/... ./proxy/... ./infra/conf/...
```

`common/geodata`、`app/dns`、`app/router` 的几个测试需要 `resources/geoip.dat`
与 `geosite.dat`（仓库不带）或外网，本地跑不过是正常的。

## Public release boundary

This fork is published separately from the upstream XTLS release channel. The upstream MPL-2.0 license and copyright notices remain in force. See [RELEASE.md](RELEASE.md) for the tag, artifact, container, compatibility, and rollback requirements.
