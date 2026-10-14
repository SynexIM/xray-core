# 数据面基准

2026-10-14T13:47:55Z · linux/arm64 · go1.27.1 · 8 CPU · `4bdd199e`

| 指标 | 数值 / p50 | p99 | max | 单位 | 样本 / 限制 |
|---|---:|---:|---:|---|---|
| vmess_uniform_20000 | 3282.4210 | 9060.7930 | 12617.9680 | µs | 1000  |
| vmess_recent_20000 | 2752.5380 | 8116.7800 | 17089.3210 | µs | 1000  |
| vmess_source_cache_20000 | 2564.0770 | 7323.1860 | 7756.7750 | µs | 1000  |
| vless_1000000 | 0.8340 | 2.0420 | 33.7090 | µs | 20000  |
| heap_per_identity_100000 | 756.4015 | — | — | B | 0  |
| rss_per_identity_100000 | 909.0662 | — | — | B | 0  |
| heap_after_delete_100000 | 47226008.0000 | — | — | B | 0  |
| rss_after_delete_100000 | 125448192.0000 | — | — | B | 0  |
| counters_after_delete_100000 | 200000.0000 | — | — | count | 0  |
| heap_per_identity_1000000 | 824.8399 | — | — | B | 0  |
| rss_per_identity_1000000 | 1007.8986 | — | — | B | 0  |
| heap_after_delete_1000000 | 540734600.0000 | — | — | B | 0  |
| rss_after_delete_1000000 | 1207611392.0000 | — | — | B | 0  |
| counters_after_delete_1000000 | 2000000.0000 | — | — | count | 0  |
| route_add_100000 | 11.4590 | 21.4829 | 21.4829 | ms | 100  |
| route_remove_100000 | 9.8093 | 20.1162 | 20.1162 | ms | 100  |
| attach_during_tick_1000000_20000_active | 0.0378 | 76.3043 | 97.6397 | ms | 432600  |
| limit_page_1000000 | 172.2173 | 437.0908 | 437.0908 | ms | 100 百万规则（比目标十万更大）；一页 50 条 |
| limit_page_100000 | 12.7457 | 30.3525 | 30.3525 | ms | 100  |
| full_config_ready_1000000 | 6.4574 | — | — | s | 0  |
| new_connections_1000000 | 426.3132 | — | — | connections/s | 0 并发 2048；失败 22528 unexpected EOF |
| new_connection_failures | 22528.0000 | — | — | count | 0  |
| new_connections_node_cpu | 93.3828 | — | — | % of 8C | 0  |
| small_packet_unlimited | 34.9089 | 49.3008 | 65.0174 | ms | 1028  |
| small_packet_loaded_2mbps | 33.9039 | 51.0950 | 61.8455 | ms | 1037  |
| udp_constant_6mbps_sent | 37500.0000 | — | — | packets | 0  |
| udp_constant_6mbps_lost | 0.0000 | — | — | packets | 0  |
| udp_constant_6mbps_send_duration | 59998.4555 | — | — | ms | 0  |
| udp_constant_6mbps_jitter | 1.5983 | 13.2934 | 124.6213 | ms | 37499  |
| udp_burst_200_sent | 200.0000 | — | — | packets | 0  |
| udp_burst_200_lost | 0.0000 | — | — | packets | 0  |
| udp_burst_200_send_duration | 10.9222 | — | — | ms | 0  |
| udp_burst_200_jitter | 0.0425 | 2.7617 | 5.3395 | ms | 199  |
| iperf_proxy_throughput | 0.5812 | — | — | Gbps | 0  |
| cpu_per_gbps | 0.3545 | — | — | cores/Gbps | 0  |
| web_single | 106.4492 | 131.7501 | 160.9188 | ms | 461  |
| web_300_downloads_20mbps | 103.2852 | 118.4203 | 123.7131 | ms | 476  |
| download_300_aggregate | 19.9952 | — | — | Mbps | 0  |
| shallow_throughput | 16.5591 | 18.2005 | 18.2005 | Mbps | 60  |
| shallow_retransmission | 0.0769 | — | — | % | 0  |
| tc_reference_throughput | 7.3399 | 11.8364 | 11.8364 | Mbps | 60  |
| tc_reference_retransmission | 0.0000 | — | — | % | 0  |
| hong_kong_node | — | — | — |  | 0 未测成：生产节点未授权变更；本轮只运行隔离容器 |
