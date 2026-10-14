# 数据面第 1 轮基准

真实 Xray 子进程与 `limit` 共用 `internal/runner` 启动工具。Go 工具只使用标准库与仓库已有的 SOCKS、gRPC、配置和校验器接口，不增加 Go 依赖。仓内没有 `testing/realnode/scale`，规模装载用现有 VLESS validator 和完整入站配置。

```sh
GOTOOLCHAIN=local GOOS=linux GOARCH=arm64 ~/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.1.darwin-arm64/bin/go build -o .perf-xray-linux ./main
GOTOOLCHAIN=local GOOS=linux GOARCH=arm64 ~/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.1.darwin-arm64/bin/go build -o .perf-runner-linux ./testing/realnode/perf
docker build -t xray-perf-local -f testing/realnode/perf/Dockerfile .
docker run --rm --privileged --network none -e XRAY_PERF_REVISION="$(git rev-parse HEAD)" -v "$PWD:/work" xray-perf-local sh testing/realnode/perf/netns.sh /work/.perf-runner-linux -seconds 60 -output testing/realnode/perf/results/after-round1
```

只建立容器内的 `perf-client → perf-node → perf-target` 命名空间。两端各加 15ms 单向 netem 延迟，合计 30ms RTT；节点出口用 fq；客户与节点之间增加透明桥接的瓶颈命名空间，避免把两种 pacer 串在同一个 qdisc 树里。两端 GSO/GRO 大小限制为 1500B。浅缓冲场景在桥的客户方向用 21Mbps netem、20 包队列，参照场景在节点 fq 上额外设 `maxrate 20mbit`。这是可重复的微突发模型，不能替代真实专线的 10 分钟验收。OrbStack 不提供 `matchall` 分类器，故不用入口 police。TC 参照组只有一条下载连接，`maxrate` 对该连接生效。

认证直接计时公共 validator，不把 AuthID 随机生成计入耗时。VMess 三组分别是均匀随机身份、同一活跃身份、同一身份同一来源；VLESS 为百万表上的随机查找。内存每组独立进程，GC 后采 heap/RSS；RSS 包含尚未归还内核的堆，不能用它代替可达对象数量。删除路径复用 `RevokeRuntimeSessions`。

巡检为百万桶、两万已 Attach 身份、60 秒按 2ms × 20 个固定到达请求建连（开放到达，包括排队时间）；除 p99 也保留最大停顿。规则分页同时测十万和百万。路由增删与完整配置就绪只记录本轮数据，后续管理与 agent 交接仍需独立验收。完整配置就绪不是现有 agent 的逐条 gRPC 交接时间。

新连接使用 64 个测试源地址和 64 个目标地址，避免客户端和节点出口的单地址 TIME_WAIT / 临时端口耗尽污染吞吐；并发默认 2048、统计开启、限速模块加载、百万 VLESS 配置。CPU 从实际节点进程 `/proc/PID/stat` 取值（Linux USER_HZ=100），按 8C 折算。iperf3 通过 SOCKS TCP 隧道，CPU/Gbps 只含节点进程，不含压测端、源站和转发隧道。

两侧采样：编译 `./testing/realnode/perf/profiled` 为 `.perf-profiled-linux`，运行 `-mode network -source connections -xray /work/.perf-profiled-linux -profiles <目录>`。它加载正式 Xray 的同一组模块，仅基准进程开启 mutex/block 采样；客户端和节点各生成 CPU/mutex/block 三个 profile。`go tool pprof -top` 与 `-http=127.0.0.1:8080` 可查看要点和火焰图。采样吞吐与无采样最终基线分开记录。

小包 64B、每次往返后等 20ms；满载与未限速分别运行。UDP 先确认整条会话可回显（排除冷邻居未解析队列），恒定码率为 1200B × 625 包/秒 × 60 秒，突发为 250Mbps 档内 10ms 发 200 包（自身约 192Mbps），发送末段用 Gosched 等待绝对时刻，报告实际发送时长。保留五层丢包、三端内核 UDP 错误、缺失序号与 qdisc 日志。`-source overspeed` 在 2Mbps 原生 UDP 入站发 600 包，验证 pipe 丢包和内核接收溢出均可观测。内核 UDP 错误由基准读 `/proc/net/snmp`，不冒充五层计数；agent 仍需采集节点内核错误。重传率取真实节点客户侧 TCP socket 的 `ss -tin` 字节重传/发送增量，热身 2 秒后测 60 秒。

`-mode` 可单独运行 auth/memory/route/runtime/page/network；逐场景原始 JSON 保留在结果目录，`-mode combine -output <目录>` 从原始数据重新生成汇总，不重新压测。未测成的场景带真实 error；香港节点需要生产测试授权，本轮不连接。三项真实专线场景按施工单暂缓。
