package protocol

// 突发信用（FR-078）。
//
// 要解决的事情很具体：客户买的是 20 Mbps，但他偶尔开个网页、下个 300 MB 的文件、
// 跑一次测速 —— 这些时候线路应该觉得很快。持续拉几十 GB 的人则应该稳定回落到基准。
// 一个固定的 20 Mbps 桶做不到第一件事，一个固定的 120 Mbps 桶做不到第二件事。
//
// 模型：
//
//	信用桶容量 burst_credit_bytes（约 1 GB）
//	扣费        **只按超出 normal_cap 的那部分字节扣** ——
//	            跑 120 Mbps、基准 20 Mbps 时，按 100 Mbps 的量消耗，不是 120。
//	            按全量扣的话，一个老老实实跑基准速度的人也会被扣光信用。
//	回补        跑得比 normal_cap 慢时，按没用满的差额回补（空闲回补）
//	峰值        随信用线性衰减：信用满 → burst_cap；信用空 → normal_cap。
//	            不做「信用一没就断崖掉回基准」，那在客户端表现为下载突然卡死一下。
//
// 明确不做的事（FR-078a）：**不识别测速站、不给测速开后门**。
// 测速显示 120 Mbps 而实际下载永远 20 Mbps，会让测速结果不再代表真实体验，
// 而且极易被用户反向识别 —— 换个非常见测速站就露馅。通用突发信用本身就能让
// 测速跑出高值，这是诚实的做法：他测出来的 120 就是他这会儿真能跑到的 120。
//
// 25ms 整形窗口不在这里，在 NodeFairScheduler.burstFor：信用决定「能跑多快」，
// 窗口决定「这一秒里怎么把它铺开」，是两件事。
const fairBurstShapingWindowMsec = 25

// burstCredit 是一个成员的信用余额。只被 recompute goroutine 访问（scheduler.mu 保护）。
type burstCredit struct {
	bytes    uint64 // 当前余额
	capacity uint64 // 上次生效的桶容量；策略变了就重新发一桶
}

// burstEnabled 报告这条策略到底有没有开突发。
// burst_cap 不高于 normal_cap 等于没开 —— 与其在别处到处判，不如在这里判一次。
func burstEnabled(p *ClassPolicy) bool {
	return p != nil && p.BurstCreditBytes > 0 && p.BurstCapBytePerSec > p.NormalCapBytePerSec
}

// settle 按本 tick 的实际用量结算信用。usedBytes 是这一 tick 双向合计的实测字节。
func (c *burstCredit) settle(p *ClassPolicy, usedBytes uint64, tickMsec uint64) {
	if !burstEnabled(p) {
		c.bytes, c.capacity = 0, 0
		return
	}
	if c.capacity != p.BurstCreditBytes {
		c.capacity = p.BurstCreditBytes
		c.bytes = p.BurstCreditBytes
	}
	allowance := p.NormalCapBytePerSec * tickMsec / 1000
	if usedBytes > allowance {
		over := usedBytes - allowance
		if over >= c.bytes {
			c.bytes = 0
		} else {
			c.bytes -= over
		}
		return
	}
	c.bytes += allowance - usedBytes
	if c.bytes > c.capacity {
		c.bytes = c.capacity
	}
}

// ceilingBytePerSec 返回这个成员当前允许的峰值（字节/秒），0 = 该 class 不设上限。
// 峰值随信用线性衰减：normal_cap + (burst_cap − normal_cap) × 剩余信用 / 桶容量。
func (c *burstCredit) ceilingBytePerSec(p *ClassPolicy) uint64 {
	if p == nil {
		return 0
	}
	if !burstEnabled(p) || c.capacity == 0 {
		return p.NormalCapBytePerSec
	}
	span := p.BurstCapBytePerSec - p.NormalCapBytePerSec
	return p.NormalCapBytePerSec + span*c.bytes/c.capacity
}
