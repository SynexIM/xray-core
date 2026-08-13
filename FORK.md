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
| `common/protocol/user.proto` | `bandwidth_bps` `conn_limit` 两个**顶层**字段 |
| `common/protocol/user.go` | `ToMemoryUser` 带上限速；`ToProtoUser` 容忍无 account 的用户 |
| `app/dispatcher/default.go` | 把限速包装器挂到 link 上；连接数上限；按站点计流量 |
| `app/dispatcher/stats.go` | `siteReadCounter` |
| `app/proxyman/command/*` | `DrainInbound` `ResumeInbound` `BatchAlterInbound` |
| `app/router/command/*` | `BatchAddRule` `BatchRemoveRule` `ListRuleFull` |
| `app/policy/*` | `user_site` 开关（按站点计流量，默认关——域名基数无上限） |
| `common/session/session.go` | `DialedRemoteAddr`（访问日志补齐目标 IP 用） |
| `features/policy/policy.go` | `Stats.UserSite` |
| `proxy/proxy.go` | `UserUpdater` 接口；受限用户强制走 buffered copy |
| `proxy/http/*` | 客户端管理与限速 |
| `proxy/vless/inbound/inbound.go` | 删用户时重置限速器与连接数 |
| `infra/conf/api.go` | 注册两个新 command service |
| `main/distro/all/all.go` | 引入两个新 app |

### 为什么限速字段放在 `User` 顶层而不是各协议的 account 里

放顶层意味着**每个协议自动都有**限速——vless、vmess、trojan、shadowsocks、
mixed 全都通过同一个 `ToMemoryUser` 拿到，不需要每个协议各自实现一个
`RuntimeLimits()` 方法。少一个协议实现，就少一个"这个协议的限速没生效"的坑。

副作用是好的：`protocol.User` 的 protobuf json tag 就是 `bandwidth_bps`，
所以凡是直接 `json.Unmarshal` 到 `protocol.User` 的协议（vless、vmess），
配置解析**自动就通了**，一行代码都不用加。

### 为什么公平限速开启时全节点放弃 splice

Linux 的 splice 在两个 socket 之间零拷贝直通，会绕过 dispatcher 挂在 link 上的
限速包装器。没有 per-user 限速的用户既逃掉公平整形，也不进活跃字节统计
（不占公平份额的分母），拥挤时会挤压守规矩的用户。

所以公平开启 = 全节点 buffered copy。代价是失去 splice 的极限吞吐。
这是产品决策：**公平 > 极限吞吐**。公平没启用时 splice 照旧。

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
