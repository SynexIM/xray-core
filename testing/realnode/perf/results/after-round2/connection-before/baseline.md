# 数据面基准

2026-10-14T16:19:28Z · linux/arm64 · go1.27.1 · 8 CPU · `8e6f7f93829df9530e7f18986602a2903ebc73d4`

| 指标 | 数值 / p50 | p99 | max | 单位 | 样本 / 限制 |
|---|---:|---:|---:|---|---|
| full_config_ready_1000000 | 6.9067 | — | — | s | 0  |
| new_connections_1000000 | 1395.9179 | — | — | connections/s | 0 并发 2048；失败 4111 read tcp 10.202.1.106:43829->10.202.1.2:19002: i/o timeout |
| new_connection_failures | 4111.0000 | — | — | count | 0  |
| new_connections_node_cpu | 64.1402 | — | — | % of 8C | 0  |
| hong_kong_node | — | — | — |  | 0 未测成：生产节点未授权变更；本轮只运行隔离容器 |
