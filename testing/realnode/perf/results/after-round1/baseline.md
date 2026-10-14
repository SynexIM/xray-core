# 数据面基准

2026-10-14T15:19:15Z · linux/arm64 · go1.27.1 · 8 CPU · `c6df4f15`

| 指标 | 数值 / p50 | p99 | max | 单位 | 样本 / 限制 |
|---|---:|---:|---:|---|---|
| vmess_uniform_20000 | 1241.2340 | 4361.8840 | 8284.8010 | µs | 1000  |
| vmess_recent_20000 | 1.4580 | 8.3750 | 995.3960 | µs | 1000  |
| vmess_source_cache_20000 | 0.4170 | 1.3750 | 20.7090 | µs | 1000  |
| vless_1000000 | 1.0410 | 5.0410 | 52.0010 | µs | 20000  |
| heap_per_identity_100000 | 745.6346 | — | — | B | 0  |
| rss_per_identity_100000 | 858.4397 | — | — | B | 0  |
| heap_after_delete_100000 | 17075608.0000 | — | — | B | 0  |
| rss_after_delete_100000 | 126222336.0000 | — | — | B | 0  |
| counters_after_delete_100000 | 0.0000 | — | — | count | 0  |
| heap_per_identity_1000000 | 778.5815 | — | — | B | 0  |
| rss_per_identity_1000000 | 919.0973 | — | — | B | 0  |
| heap_after_delete_1000000 | 162037528.0000 | — | — | B | 0  |
| rss_after_delete_1000000 | 1156657152.0000 | — | — | B | 0  |
| counters_after_delete_1000000 | 0.0000 | — | — | count | 0  |
| route_add_100000 | 11.7277 | 22.9912 | 22.9912 | ms | 100  |
| route_remove_100000 | 9.9490 | 22.4030 | 22.4030 | ms | 100  |
| attach_during_tick_1000000_20000_active | 0.0659 | 1.1852 | 18.8497 | ms | 388200  |
| limit_page_1000000 | 0.0002 | 0.0322 | 0.0322 | ms | 100 百万规则（比目标十万更大）；一页 50 条 |
| limit_page_100000 | 0.0001 | 0.0090 | 0.0090 | ms | 100  |
| full_config_ready_1000000 | 6.4232 | — | — | s | 0  |
| new_connections_1000000 | 424.8732 | — | — | connections/s | 0 并发 2048；失败 22213 unexpected EOF |
| new_connection_failures | 22213.0000 | — | — | count | 0  |
| new_connections_node_cpu | 93.3975 | — | — | % of 8C | 0  |
| small_packet_unlimited | 37.1034 | 47.4914 | 50.9749 | ms | 981  |
| small_packet_loaded_2mbps | 33.3404 | 42.7113 | 76.1144 | ms | 1076  |
| udp_constant_6mbps_sent | 37500.0000 | — | — | packets | 0  |
| udp_constant_6mbps_lost | 0.0000 | — | — | packets | 0  |
| udp_constant_6mbps_send_duration | 60001.2140 | — | — | ms | 0  |
| udp_constant_6mbps_jitter | 1.5986 | 8.9005 | 20.5967 | ms | 37499  |
| udp_burst_200_sent | 200.0000 | — | — | packets | 0  |
| udp_burst_200_lost | 48.0000 | — | — | packets | 0  |
| udp_burst_200_send_duration | 12.7953 | — | — | ms | 0  |
| udp_burst_200_jitter | 0.0493 | 4.1040 | 6.0503 | ms | 151  |
| iperf_proxy_throughput | 0.5999 | — | — | Gbps | 0  |
| cpu_per_gbps | 0.2030 | — | — | cores/Gbps | 0  |
| web_single | 101.0836 | 115.0142 | 133.4741 | ms | 487  |
| web_300_downloads_20mbps | 102.7147 | 117.7000 | 127.5437 | ms | 480  |
| download_300_aggregate | 19.9888 | — | — | Mbps | 0  |
| shallow_throughput | 18.0847 | 19.2840 | 19.2840 | Mbps | 60  |
| shallow_retransmission | 0.0000 | — | — | % | 0  |
| tc_reference_throughput | 8.2142 | 12.8266 | 12.8266 | Mbps | 60  |
| tc_reference_retransmission | 0.0000 | — | — | % | 0  |
| hong_kong_node | — | — | — |  | 0 未测成：生产节点未授权变更；本轮只运行隔离容器 |
