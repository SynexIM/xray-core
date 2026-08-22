# 这个 fork 改了什么

基线：**XTLS/Xray-core v26.7.28**（2026-07-28）。

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
common/buf/fairlimit.go     公平份额整形
common/buf/limit.go         每用户带宽整形
common/protocol/user_limits.go     每用户限速状态
common/protocol/user_conns.go      每用户连接数
common/protocol/node_fairshare.go  节点级自适应带宽调度器
common/protocol/burst_credit.go    突发信用
proxy/http/users.go         给 http 协议补上客户端（email）管理
```

## 改动（在上游文件里加东西）

| 文件 | 加了什么 |
|---|---|
| `common/protocol/user.proto` | `bandwidth_bps` `conn_limit` 两个**顶层**字段；双速率的 `committed_bps` `committed_burst_bytes`；争抢等级 `class` |
| `common/protocol/user.go` | `ToMemoryUser` 带上限速；`ToProtoUser` 容忍无 account 的用户 |
| `app/dispatcher/default.go` | 把限速包装器挂到 link 上（单速率一个桶、双速率两个）；连接数上限；按站点计流量 |
| `app/dispatcher/stats.go` | `siteReadCounter` |
| `app/proxyman/command/*` | `DrainInbound` `ResumeInbound` `BatchAlterInbound`；`AddUsersOperation` `RemoveUsersOperation`；卸载时清运行态 |
| `app/router/command/*` | `BatchAddRule` `BatchRemoveRule` `ListRuleFull` |
| `app/reverse/*` | Reverse 加锁 + 存 dispatcher/ohm，开放 bridge/portal 的增删查 |
| `app/policy/*` | `user_site` 开关（按站点计流量，默认关——域名基数无上限） |
| `common/session/session.go` | `DialedRemoteAddr`（访问日志补齐目标 IP 用） |
| `features/policy/policy.go` | `Stats.UserSite` |
| `proxy/proxy.go` | `UserUpdater` 接口；`BatchUserManager` 接口；受限用户强制走 buffered copy |
| `proxy/http/*` | 客户端管理与限速 |
| `proxy/vless/inbound/inbound.go` | 删用户时重置限速器与连接数 |
| `proxy/socks/config.proto` `proxy/http/config.proto` | `UserAccount` 也带双速率字段与 `class` |
| `proxy/shadowsocks_2022/config.proto` | `RelayDestination` 带四个限速字段与 `class`（relay 没有 User 消息） |
| `proxy/shadowsocks_2022/inbound_multi.go` | email→下标索引；`AddUsers`/`RemoveUsers` 批量入口 |
| `proxy/shadowsocks/validator.go` `proxy/vmess/validator.go` | email→下标索引，`Del`/`Remove` 从 O(N) 变 O(1) |
| `proxy/shadowsocks_2022/inbound_relay.go` | 每个 destination 一个长期 `MemoryUser`；顺带补上漏传的 `level` |
| `proxy/http/users.go` | `UserStore` 把双速率翻进 `MemoryUser` |
| `infra/conf/*.go` | 各协议解析 `committed_bps` `committed_burst_bytes` `class` |
| `infra/conf/api.go` | 注册三个新 command service |
| `main/distro/all/all.go` | 引入新 app 与 command service |

### 为什么限速字段放在 `User` 顶层而不是各协议的 account 里

放顶层意味着**每个协议自动都有**限速——vless、vmess、trojan、shadowsocks、
mixed 全都通过同一个 `ToMemoryUser` 拿到，不需要每个协议各自实现一个
`RuntimeLimits()` 方法。少一个协议实现，就少一个"这个协议的限速没生效"的坑。

副作用是好的：`protocol.User` 的 protobuf json tag 就是 `bandwidth_bps`，
所以凡是直接 `json.Unmarshal` 到 `protocol.User` 的协议（vless、vmess），
配置解析**自动就通了**，一行代码都不用加。后来加的 `committed_bps` /
`committed_burst_bytes` 同样白拿这个好处——不过这一点不靠假设，
`infra/conf/limits_matrix_test.go` 逐协议实际验证过。

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

### 双速率：PIR / CIR / CBS，以及为什么 CBS 默认是一天的承诺量

专线卖的是「承诺速率 + 允许突发」，一个桶表达不了：按峰值卖成本兜不住，
按承诺卖客户觉得慢。所以限速器是**一串桶**，流量依次通过每一个：

```
bandwidth_bps          PIR  峰值速率，突发能到多快        bit/s，0 = 不限
committed_bps          CIR  承诺速率，长期稳定给多少      bit/s，0 = 不设
committed_burst_bytes  CBS  能以峰值速率花掉多少额度      字节，0 = 自动
```

`committed_bps = 0` 时只有峰值桶，单速率行为**一个字节都没变**。
设了 CIR（且 CIR < PIR）就在峰值桶后面串一个更深的承诺桶：新连接上来时
承诺桶是满的，立刻放行，只有峰值桶在排队 → 跑 PIR；CBS 花完后承诺桶开始
按 CIR 滴令牌 → 自然落到 CIR。不需要任何额外状态机。

**CBS 留空时默认 = 一天的承诺量（`bitsToBytes(CIR) × 86400`）。**
CBS 在这里是业务额度，不是防锯齿的窗口。承诺速率卖的是「你每天至少有这么多」，
与之配套的突发额度自然就是「这一天的承诺量你可以随时以峰值速率花掉」——
客户白天猛用晚上不用，或者反过来，都不吃亏，而一天之内的总量仍被 CIR 兜住。
窗口更短会把额度切碎（客户感觉「刚快一下就掉速」），
更长会让一次异常爆发吃掉后面好几天的额度。

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

### 节点级限速是一个自适应带宽调度器，不是均分

`node_fairshare.go` 原来是 `share = avail / len(active)` 纯人头均分。它有两个
说不过去的地方：

一是**只想要 0.17 Mbps 的人照样占着一整份**。480 Mbps 的节点、500 个客户，
其中 300 个只在挂着不怎么用，剩下 200 个人也只能拿人头份额 0.96 Mbps，
另外那 300 份基本烂在手里。

二是**地板无条件生效**。原来 `share < hard` 时把每个人都抬到硬地板，不管
`hard × 人数` 是否给得起：160,000 B/s 的节点、50 个活跃用户、16,384 的硬地板 →
调度器发出去 819,200 B/s，是节点上限的 5.1 倍。发出的额度比水管还大，
它就不再是瓶颈，真实排队跑到上游运营商的缓冲区里去了——那里我们既看不见也
控制不了。这不是 bug 而是「只慢不断连」的刻意取舍，但它的代价没被写下来。
2026-08-22 裁定改选「节点总出口守得住」。

现在是 **work-conserving 加权 max-min 公平（注水法）**，对标运营商 BNG 里的
Subscriber-aware Hierarchical QoS：

```
每 tick 把活跃成员分两类
  satisfied   本 tick 从未因等令牌阻塞  →  demand = 实测吞吐（他就要这么多）
  backlogged  阻塞过                    →  还想要更多，具体多少不必猜
分配
  1  地板先扣掉（前提：地板×活跃人数 ≤ root_cap，给不起就不给）
  2  satisfied 按实测吞吐（+1/8 抬头）钉住，扣掉
  3  剩下的池子在 backlogged 里按 class weight 分
  4  谁分到超过自己天花板就钉住、多的还池，回 3，直到无人新饱和
```

**关键简化：不需要预测「他想要多少」，只需要知道「他够不够」。**
够不够靠 `FairLimitReader`/`Writer` 的 `onBlocked` 回调测——这一 tick 内一次都
没为等令牌而阻塞过，就是够了。不猜数值、不做 DPI、不做流量指纹。

额度总量契约：**只要调度器进入约束态**（有成员被权重份额压住，而不是拿到自己
想要的全部），Σ allocation ≤ root_cap，一个字节都不多发。反过来，没人被压住时
每人发的是各自天花板，合计可以大于 root_cap——那不是超发，那是 work-conserving
的定义，因为没人真的想要那么多。

三档地板，逐档退到给得起为止：`max(class 地板, 软地板)` → 硬地板 → 不给。
地板还被自己的天花板夹住：给一个只买了 8KB/s 的人 16KB/s 的地板毫无意义，
他跑不掉，只会白白吃掉别人的份额。

拥塞滞回：利用率不过上阈值**根本不削速**，每人跑自己的天花板；回落到下阈值
并连续 N 个 tick 才退出，避免在 89%/91%/89% 之间反复抖动。阈值留空 = 不做拥塞
判定（永远公平模式，等于改造前的行为）。

**`normal_cap` 不是保证带宽，绝不能实现成 CIR。** 500 个在线客户 × 20 Mbps =
10 Gbps，物理只有 500 Mbps，数学上不可能保证。它的语义是「机器不挤的时候你能
一直跑到这个速度」。`node_fairshare_test.go` 里有一条测试专门守住这件事：
10 个人挤 6 MB/s 时**人人都拿不到** `normal_cap`。

**xray 只管「带宽怎么分」，管不了「排队延迟」。** xray 是在用户态对已经读进内存
的 buffer 整形，做不了 AQM。直播在拥塞时的体验主要取决于延迟而不是带宽，
所以节点装机还必须一并下发 `tc qdisc replace dev <wan> root cake bandwidth
<root_cap>`，且 `root_cap` 两边同值。只做加权公平，直播在满载时照样卡。

### 突发信用：只按超出基准的那部分扣，且不给测速开后门

客户买的是 20 Mbps，但他偶尔开个网页、下个 300 MB 文件、跑一次测速——这些时候
线路应该觉得很快；持续拉几十 GB 的人则应该稳定回落到基准。一个固定的桶做不到
两头兼顾，所以有了 `burst_credit.go`：

```
桶容量   burst_credit_bytes（约 1 GB）
扣费     只按超出 normal_cap 的那部分字节扣 —— 跑 120 Mbps、基准 20 Mbps 时
         按 100 Mbps 的量消耗。按全量扣的话，老老实实跑基准的人也会被扣光。
回补     跑得比 normal_cap 慢时按没用满的差额回补
峰值     随信用线性衰减：信用满 → burst_cap，信用空 → normal_cap。
         不做断崖回落，那在客户端表现为下载突然卡死一下。
整形     有突发策略的成员用 25ms 窗口（普通成员 125ms）。burst_cap 常是基准的
         5~6 倍，用 125ms 窗口会让它一次倾泻近 2MB，整形就成了摆设。
```

**明确不做：识别测速站然后偷偷解除限速。** 测速显示 120 Mbps 而实际下载永远
20 Mbps，会让测速结果不再代表真实体验，且极易被用户反向识别——换个非常见测速站
就露馅。通用突发信用本身就能让测速跑出高值，这是诚实的做法：他测出来的 120
就是他这会儿真能跑到的 120。

### class（= SKU）策略表走 `SetClassPolicy`，不另造通道

class 名挂在 `User.class` 上随实例下发，策略表（weight / normal_cap /
burst_cap / burst_credit / floor_ratio）是**运营参数**，走
`app.fairshare.command` 的 `SetClassPolicy` 整份声明式替换，不进客户界面。
直播 weight 高于短视频；实测拥塞时两者拿到的带宽正好是 weight 比，
且短视频不被饿死；直播空闲时短视频能吃满自己的 `normal_cap`。

class 必须贯穿**全部八个协议**的配置路径。少覆盖一个的表现极其隐蔽：
配置写了、面板显示了、保存也成功了，xray 解析时静默丢弃，这个客户在节点上落回
同权重兜底，卖出去的直播优先级不生效而账面上是生效的。
`infra/conf/limits_matrix_test.go` 逐协议钉住这件事。

### ⚠️ 单位陷阱：同一个 `_bps` 后缀，两处含义差 8 倍

```
common/protocol/user.proto        bandwidth_bps / committed_bps   比特/秒
app/fairshare/command/*.proto     avail_bps / *_floor_bps         字节/秒
```

前者是业务单位（面板按 Mbps 展示后 ×1e6），后者是限速器单位。唯一的换算点是
`user_limits.go` 的 `bitsPerSecondToRuntimeBytesPerSecond`。

这两组历史字段**不改名**（改名会断掉已经在跑的 node-agent），改为在两份 proto
里各自写死语义，并由 `common/protocol/node_fairshare_units_test.go` 与
`app/fairshare/command/command_units_test.go` 两组断言测试钉住那个 8 倍差。
谁要是「顺手统一成同一单位」，那两组测试会红——而线上的症状会是
「节点被掐到 1/8 速度」，从现象倒查回来要几天。

**新增的速率字段一律带 `_bit_per_sec` / `_byte_per_sec` 后缀，不许再用裸 `_bps`。**

### 不许有默认带宽

```
每客户端 bandwidth_bps / committed_bps   0 → 不套桶
节点 avail_bps                           0 → 整个节点公平关闭
软地板 soft_floor_bps                    0 → 无软地板
硬地板 hard_floor_bps                    0 → 无硬地板
class 不配                               → 同权重、无 class 上限、无突发
```

后两条是这次改的：原来 0 会被悄悄换成 `500_000/8` 和 `16*1024` 两个魔数，
运营看不出来自己其实开了地板。**0 就是「无地板」，不是「用默认值」。**
代价要在面板上写明：开了节点公平又不设硬地板，极端拥挤时用户可能被压到接近 0
而不只是变慢——这必须是运营明知的选择，不能由代码替他默默决定。

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

控制面给客户换入口是常规操作。配置只在启动时读一次的话，换一个客户的入口
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

### 公平调度里「这个用户的天花板」怎么算

`fairOwnLimitBytesPerSecond` 返回的 0 在所有调用点（`Member` 建桶与 `ceilingFor`）
都是「这个用户没有自己的上限」的意思。所以它必须返回**实际天花板**：
`bandwidth_bps` 非 0 就用它，否则退到 `committed_bps`，两个都是 0 才返回 0。

只读 `bandwidth_bps` 会让「只买了承诺速率」的客户（PIR=0、CIR>0，语义上就是单速率
CIR）在公平分配里被当成无天花板：拥挤时他分到一份自己根本跑不满的份额
（per-user 承诺桶还压着他），这部分节点容量就空转了。这跟
「PIR = 0 且设了 CIR = 单速率 CIR」的双速率语义是同一件事。

双速率用户的天花板是 **PIR**，不是 CIR——CIR 只在 CBS 花完后拉低长期均值，
不是他能跑到的最快速度。

## 有意不改的

- `app/router/command/command.go:170` 的 context 泄漏（`context.WithTimeout`
  的 cancel 被丢弃）。这是**上游自带**的，v26.7.28 和 main 都一样。
  改它只会增加 rebase 的冲突面，而泄漏 4 秒后自己就释放了。
- `app/stats` 与 `features/stats`：上游已经把 `GetOrRegisterCounter` 提升成
  Manager 接口的方法，比 fork 原来的包级辅助函数更好，所以**用上游的**。

## 怎么跟上游

```bash
git remote add upstream https://github.com/XTLS/Xray-core.git
git fetch upstream --tags
git rebase <新 tag>
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
