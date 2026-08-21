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
common/protocol/node_fairshare.go  节点级公平调度器
proxy/http/users.go         给 http 协议补上客户端（email）管理
```

## 改动（在上游文件里加东西）

| 文件 | 加了什么 |
|---|---|
| `common/protocol/user.proto` | `bandwidth_bps` `conn_limit` 两个**顶层**字段；双速率的 `committed_bps` `committed_burst_bytes` |
| `common/protocol/user.go` | `ToMemoryUser` 带上限速；`ToProtoUser` 容忍无 account 的用户 |
| `app/dispatcher/default.go` | 把限速包装器挂到 link 上（单速率一个桶、双速率两个）；连接数上限；按站点计流量 |
| `app/dispatcher/stats.go` | `siteReadCounter` |
| `app/proxyman/command/*` | `DrainInbound` `ResumeInbound` `BatchAlterInbound` |
| `app/router/command/*` | `BatchAddRule` `BatchRemoveRule` `ListRuleFull` |
| `app/reverse/*` | Reverse 加锁 + 存 dispatcher/ohm，开放 bridge/portal 的增删查 |
| `app/policy/*` | `user_site` 开关（按站点计流量，默认关——域名基数无上限） |
| `common/session/session.go` | `DialedRemoteAddr`（访问日志补齐目标 IP 用） |
| `features/policy/policy.go` | `Stats.UserSite` |
| `proxy/proxy.go` | `UserUpdater` 接口；受限用户强制走 buffered copy |
| `proxy/http/*` | 客户端管理与限速 |
| `proxy/vless/inbound/inbound.go` | 删用户时重置限速器与连接数 |
| `proxy/socks/config.proto` `proxy/http/config.proto` | `UserAccount` 也带双速率字段 |
| `proxy/shadowsocks_2022/config.proto` | `RelayDestination` 带四个限速字段（relay 没有 User 消息） |
| `proxy/shadowsocks_2022/inbound_relay.go` | 每个 destination 一个长期 `MemoryUser`；顺带补上漏传的 `level` |
| `proxy/http/users.go` | `UserStore` 把双速率翻进 `MemoryUser` |
| `infra/conf/*.go` | 各协议解析 `committed_bps` `committed_burst_bytes` |
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

`fairOwnLimitBytesPerSecond` 返回的 0 在三个调用点（`Member` 建桶、`recompute`
活跃分支、`applyOwn` 非活跃分支）都是「这个用户没有自己的上限」的意思。
所以它必须返回**实际天花板**：`bandwidth_bps` 非 0 就用它，否则退到
`committed_bps`，两个都是 0 才返回 0。

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
