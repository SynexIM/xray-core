# xray-core fork 协作协议

这是 SynexIM 维护的 Xray-core fork，服务专线数据面（`targetProject: ipline`）。
工作区总协议见 `../AGENTS.md`；规格权威指针见 `../specs/ACTIVE.md`。
fork 相对上游改了什么、为什么改，唯一记录处是本仓 `FORK.md`——改动落地必须同步它。

## 本仓铁律

1. **先量再改**：性能话题一律"先测量、后修改、再复测"，两个数字都写进提交说明。
   凭直觉的优化一律不收。真节点验收工具在 `testing/realnode/`。
2. **fork 漂移是长期税**：能在 3x-ui / 控制面侧解决的不碰本仓；改动最小化，
   为的是还能跟上游 rebase。
3. **节点契约不进商业词汇**：user/order/price 等业务概念不得出现在 API 与配置面
   （已有提交 `a06e29d3` 为例）。
4. 限速/公平相关改动必须守住既有测试矩阵（八协议贯穿、UDP/XUDP 挂载点、
   "开 N 条连接不放大额度"）；REALITY 依赖用自家 fork（go.mod replace），
   上游 XTLS/REALITY#33 合并后删 replace。
5. 开源仓：发布走自家 Release 通道，不与上游混用；不引入闭源依赖。
