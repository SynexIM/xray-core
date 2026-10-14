# 本地补丁

来源：github.com/xtls/reality v0.0.0-20260908062103-8cdf7bf9c7f0（原样拷贝），只改 `tls.go` 一处：

- 上游从 Xray 26.9.8 起要求客户端的 ClientHello 必须先带 X25519MLKEM768 key share，否则当作未认证、回落到伪装站。
- 小火箭（Shadowrocket，fp=chrome）、Stash、Karing 等客户端只发 X25519，于是 VLESS-Reality 全部连不上（服务端日志 `REALITY: processed invalid connection ... authentication failed`，小火箭报 `read error => -4`）。
- 补丁恢复旧行为：两种 key share 至少有一个就继续认证；带 ML-KEM 的客户端仍按原逻辑处理。

升级上游 reality 时：重新拷贝新版本，把同一处改回来；或上游提供开关后删掉本目录和 go.mod 的 replace。
