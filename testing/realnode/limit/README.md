# 真实进程限速验收

本工具启动参数指定的真实 Xray 二进制、本机无限 HTTP 源与上传汇点，经 SOCKS 或 VLESS Vision 传输，按目的端实际字节每秒输出 `t=12s rate=4.01Mbps`。验收失败退出 1，不用内存桶替代数据面。

```bash
go build -o xray ./main
go run ./testing/realnode/limit -xray ./xray -scenario ladder -direction upload -seconds 60
```

| 场景 | 默认配置与验收 | 建议秒数 |
|---|---|---|
| ladder | 10Mbps 5s → 4Mbps 20s → 1Mbps；两个拐点误差不超过 1s | 60 |
| shared | 4 个账户共享一条 8Mbps 规则，测聚合速率 | 10 |
| outbound | 5 个 8Mbps 账户桶叠加 20Mbps 出口桶，测总量和各账户上限 | 10 |
| hotswap | 第 5 秒真实 CLI 调用 PutProfiles，2→5Mbps；第 6 秒已变速，整个测量期间不断流 | 10 |
| sourceips | 两个来源流与第三来源 probe；第三个先被拒，第 10 秒经 KillUserConnections 断开前两条，第 32 秒第三个可用 | 35 |
| vision | SOCKS→VLESS Vision/TLS→HTTPS；5Mbps 上限、每秒速率与用户计数校验 | 10 |

可选参数：`-direction upload|download`、`-seconds`、`-unlimited`（不加载 limit，测改造前基线）、`-debug`、`-source-ips 127.0.0.2,127.0.0.3,127.0.0.4`。Vision 只用 download，其他场景支持双向。窗口均值允许 ±12% 的采样偏差；该阈值用于本机验收，不是节点超售参数。来源 IP 与 Linux splice 相关验收建议在 Linux 执行，系统须允许绑定所选来源地址。

工具仅放行本机 HTTP 源的准确 IP/端口，Vision 用临时自签证书与证书钉扎连接。账户口令和 UUID 为本机测试夹具，无外部服务依赖；配置和日志位于工具自己的临时目录，结束删除。不会管理已有 Xray 进程。

在 macOS 可用无外网容器运行 Linux ARM64 验收（预先具备 alpine:3.22）：

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o .limit-xray-linux.tmp ./main
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o .limit-realnode-linux.tmp ./testing/realnode/limit
docker run --rm --network none \
  --mount type=bind,source="$PWD",target=/work,readonly \
  --entrypoint /work/.limit-realnode-linux.tmp alpine:3.22 \
  -xray /work/.limit-xray-linux.tmp -scenario vision -direction download -seconds 10
```

本机 TCP 阶梯与 Linux Vision 通过仍不能证明 HY2 Brutal、八协议公网质量或实际节点容量，需另做真实节点验收。
