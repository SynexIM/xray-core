package protocol_test

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	. "github.com/xtls/xray-core/common/protocol"
	"golang.org/x/time/rate"
)

// 双速率（PIR / CIR / CBS）的行为测试。
//
// 这里**不用 sleep**。rate.Limiter 的每个方法都能显式传时间戳（AllowN / TokensAt），
// 所以整段跑在一个虚拟时钟上：结果完全确定，跑一万遍都一样，也不会在 CI 上因为
// 机器忙一下就变红。靠 sleep 测限速，测的其实是机器忙不忙。

const (
	mbps  = 1_000_000 // bit/s
	mbyte = 1_000_000 // byte
)

// drainer 模拟一个「有多少吃多少」的贪心发送方：每一步取所有桶都拿得出的最大
// 字节数，扣掉，累计。这就是真实链路上一个满速连接的样子。
//
// 它是**有游标的**，只能连续往前跑。跳过一段时间等于让桶白攒令牌，
// 那样量出来的「稳态速率」是假的（会把攒下的 burst 算进去）。
type drainer struct {
	limiters []*rate.Limiter
	start    time.Time
	step     time.Duration
	cursor   time.Duration
}

// until 从上次停下的地方连续跑到 to，返回这一段里实际通过的字节数。
func (d *drainer) until(to time.Duration) uint64 {
	var total uint64
	for ; d.cursor < to; d.cursor += d.step {
		now := d.start.Add(d.cursor)
		avail := -1.0
		for _, l := range d.limiters {
			if tokens := l.TokensAt(now); avail < 0 || tokens < avail {
				avail = tokens
			}
		}
		n := int(avail)
		if n <= 0 {
			continue
		}
		for _, l := range d.limiters {
			if !l.AllowN(now, n) {
				panic("模拟写错了：刚报告拿得出 n，转头就拿不出")
			}
		}
		total += uint64(n)
	}
	return total
}

func newDrainer(t *testing.T, u *MemoryUser) *drainer {
	t.Helper()
	limiters, _ := u.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if len(limiters) == 0 {
		t.Fatal("这个用户应该有限速桶，却一个也没有")
	}
	return &drainer{
		limiters: limiters,
		// 起点取「桶已经攒满」的时刻：rate.Limiter 零值的 last 是零时间，
		// 到 now 的间隔大到足以灌满，这跟线上新建限速器的状态一致。
		start: time.Now(),
		step:  10 * time.Millisecond,
	}
}

func rateOver(bytes uint64, window time.Duration) float64 {
	return float64(bytes) / window.Seconds()
}

// 核心用例：PIR = 10MB/s，CIR = 1MB/s，CBS = 5MB。
// 期望：开头跑在 PIR 附近，5MB 突发额度花完后稳定落到 CIR 附近。
func TestDualRatePeakThenCommitted(t *testing.T) {
	user := &MemoryUser{
		Email:               "dual@example.test",
		BandwidthBps:        80 * mbps, // PIR 10 MB/s
		CommittedBps:        8 * mbps,  // CIR 1 MB/s
		CommittedBurstBytes: 5 * mbyte, // CBS 5 MB
	}
	d := newDrainer(t, user)

	// 第一阶段：前 500ms。CBS 还没花完，卡住流量的是峰值桶，速率≈PIR。
	// 略高于 PIR 是对的——峰值桶自己那 1/8 秒的 burst 也在这个窗口里被花掉了。
	peakBytes := d.until(500 * time.Millisecond)
	peakRate := rateOver(peakBytes, 500*time.Millisecond)
	t.Logf("突发阶段(0~500ms)：%.2f MB（%.2f MB/s）", float64(peakBytes)/mbyte, peakRate/mbyte)
	if peakRate < 9*mbyte {
		t.Errorf("突发阶段没跑到 PIR：%.2f MB/s，期望 ≈10 MB/s\n"+
			"  后果：客户买了峰值速率却一开始就跑不满，双速率白做", peakRate/mbyte)
	}
	if peakRate > 13*mbyte {
		t.Errorf("突发阶段超出 PIR 太多：%.2f MB/s——峰值桶没起作用？", peakRate/mbyte)
	}

	// 中间这段照跑不误（不能跳，见 drainer 注释）。
	midBytes := d.until(5 * time.Second)

	// 第二阶段：第 5~10 秒。CBS 早已耗尽，承诺桶成为瓶颈，速率应稳在 CIR。
	steadyBytes := d.until(10 * time.Second)
	steadyRate := rateOver(steadyBytes, 5*time.Second)
	t.Logf("稳态阶段(5~10s)：%.2f MB（%.3f MB/s）", float64(steadyBytes)/mbyte, steadyRate/mbyte)
	if steadyRate < 0.95*mbyte || steadyRate > 1.05*mbyte {
		t.Errorf("额度耗尽后没落到 CIR：%.3f MB/s，期望 ≈1 MB/s\n"+
			"  低了 = 客户拿不到承诺速率；高了 = 承诺桶没串上，卖多少都按峰值跑", steadyRate/mbyte)
	}

	// 总量守恒：10 秒最多放行 CBS + CIR×10s ≈ 15MB（外加峰值桶那点起始 burst）。
	// 明显超出说明某个桶被绕过去了。
	total := peakBytes + midBytes + steadyBytes
	t.Logf("10 秒总量：%.2f MB", float64(total)/mbyte)
	if total > 16*mbyte {
		t.Errorf("10 秒放行 %.2f MB，超过 CBS+CIR×10s≈15MB——有桶被绕过", float64(total)/mbyte)
	}
	if total < 14*mbyte {
		t.Errorf("10 秒只放行 %.2f MB，少于应得的 ≈15MB——限过头了", float64(total)/mbyte)
	}
}

// 单速率不回归：只设 bandwidth_bps 时，桶的数量、速率、burst 与改动前完全一致，
// 长时间稳态速率就是 PIR，不会莫名其妙落到某个更低的值。
func TestSingleRateUnchanged(t *testing.T) {
	user := &MemoryUser{
		Email:        "single@example.test",
		BandwidthBps: 80 * mbps, // 10 MB/s
	}
	limiters, burst := user.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if len(limiters) != 1 {
		t.Fatalf("不设承诺速率时应该只有一个桶，实际 %d 个", len(limiters))
	}
	wantLimiter, wantBurst := buf.NewRateLimiter(10 * mbyte)
	if limiters[0].Limit() != wantLimiter.Limit() {
		t.Errorf("单速率桶的速率变了：%v，期望 %v", limiters[0].Limit(), wantLimiter.Limit())
	}
	if limiters[0].Burst() != wantBurst || burst != wantBurst {
		t.Errorf("单速率桶的 burst 变了：桶 %d / 返回 %d，期望 %d", limiters[0].Burst(), burst, wantBurst)
	}

	d := newDrainer(t, user)
	d.until(5 * time.Second)
	steady := rateOver(d.until(10*time.Second), 5*time.Second)
	t.Logf("单速率稳态(5~10s)：%.2f MB/s", steady/mbyte)
	if steady < 9.5*mbyte || steady > 10.5*mbyte {
		t.Errorf("单速率稳态 %.2f MB/s，期望 ≈10 MB/s", steady/mbyte)
	}
}

// CBS 留空时默认一天的承诺量。这是商业默认值，改了它等于改了所有只填 CIR 的客户的套餐。
func TestCommittedBurstDefaultsToOneDay(t *testing.T) {
	user := &MemoryUser{
		Email:        "default-cbs@example.test",
		BandwidthBps: 80 * mbps,
		CommittedBps: 8 * mbps, // 1 MB/s
	}
	limiters, _ := user.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if len(limiters) != 2 {
		t.Fatalf("PIR>CIR>0 时应有两个桶，实际 %d 个", len(limiters))
	}
	wantBurst := int64(mbyte) * 86400
	if got := int64(limiters[1].Burst()); got != wantBurst {
		t.Errorf("CBS 默认值 = %d 字节，期望一天的承诺量 %d 字节", got, wantBurst)
	}
}

// CIR 不比 PIR 松就没有意义：串上去只会平白多一次 WaitN。
func TestCommittedAtOrAbovePeakIsIgnored(t *testing.T) {
	for _, cir := range []uint64{80 * mbps, 160 * mbps} {
		user := &MemoryUser{
			Email:        "cir-too-high@example.test",
			BandwidthBps: 80 * mbps,
			CommittedBps: cir,
		}
		limiters, _ := user.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
		if len(limiters) != 1 {
			t.Errorf("CIR=%d ≥ PIR 时应退化成单速率，实际 %d 个桶", cir, len(limiters))
		}
		user.ResetRuntimeLimiter()
	}
}

// 只填 CIR 不填 PIR：当单速率 CIR 处理，CBS 忽略。
// 配错的后果必须是「限住了」而不是「放开了」——所以这里绝不能拿 CBS 当 burst，
// 否则一个只填了 CIR 的用户会先白拿一大截不限速额度。
func TestCommittedWithoutPeakIsPlainSingleRate(t *testing.T) {
	user := &MemoryUser{
		Email:               "cir-only@example.test",
		CommittedBps:        8 * mbps,    // 1 MB/s
		CommittedBurstBytes: 500 * mbyte, // 故意给一个很大的 CBS
	}
	limiters, _ := user.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if len(limiters) != 1 {
		t.Fatalf("只设 CIR 时应只有一个桶，实际 %d 个", len(limiters))
	}
	_, wantBurst := buf.NewRateLimiter(mbyte)
	if got := limiters[0].Burst(); got != wantBurst {
		t.Errorf("只设 CIR 时 burst = %d，期望默认 1/8 秒窗口 %d（CBS 必须被忽略）", got, wantBurst)
	}

	d := newDrainer(t, user)
	d.until(2 * time.Second)
	steady := rateOver(d.until(7*time.Second), 5*time.Second)
	if steady < 0.95*mbyte || steady > 1.05*mbyte {
		t.Errorf("只设 CIR 的稳态 %.3f MB/s，期望 ≈1 MB/s", steady/mbyte)
	}
}

// 三个字段任一变化都要重建桶。只比 bandwidth_bps 的话，
// 面板上改 CIR/CBS 会「保存成功但节点上不生效」，而且没人看得出来。
func TestLimiterRebuiltWhenAnyRateFieldChanges(t *testing.T) {
	user := &MemoryUser{
		Email:        "rebuild@example.test",
		BandwidthBps: 80 * mbps,
		CommittedBps: 8 * mbps,
	}
	before, _ := user.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if len(before) != 2 {
		t.Fatalf("期望两个桶，实际 %d 个", len(before))
	}

	user.CommittedBps = 16 * mbps
	afterCIR, _ := user.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if afterCIR[1] == before[1] {
		t.Error("改了 committed_bps 却复用旧桶——限速改了不生效")
	}

	user.CommittedBurstBytes = 42 * mbyte
	afterCBS, _ := user.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if afterCBS[1] == afterCIR[1] {
		t.Error("改了 committed_burst_bytes 却复用旧桶——限速改了不生效")
	}
	if got := afterCBS[1].Burst(); got != 42*mbyte {
		t.Errorf("新 CBS 没吃进去：burst = %d，期望 %d", got, 42*mbyte)
	}

	user.BandwidthBps = 160 * mbps
	afterPIR, _ := user.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if afterPIR[0] == afterCBS[0] {
		t.Error("改了 bandwidth_bps 却复用旧桶——限速改了不生效")
	}
	user.ResetRuntimeLimiter()
}

// splice 会绕过 dispatcher 挂的限速包装器，所以「身上有任何限制」的用户都必须
// 被判出来强制走 buffered copy。只填 CIR 的用户 BandwidthBps 是 0——
// 只看 BandwidthBps 的话他会被当成不受限，走 splice，限速配了却一个字节都限不住。
func TestHasRuntimeLimitsCoversEveryField(t *testing.T) {
	cases := []struct {
		name string
		user *MemoryUser
		want bool
	}{
		{"什么都不设", &MemoryUser{}, false},
		{"只设峰值", &MemoryUser{BandwidthBps: 1}, true},
		{"只设连接数", &MemoryUser{ConnLimit: 1}, true},
		{"只设承诺速率", &MemoryUser{CommittedBps: 1}, true},
		{"只设突发额度", &MemoryUser{CommittedBurstBytes: 1}, false}, // CBS 单独存在不构成限制
		{"nil 用户", nil, false},
	}
	for _, c := range cases {
		if got := c.user.HasRuntimeLimits(); got != c.want {
			t.Errorf("%s：HasRuntimeLimits() = %v，期望 %v", c.name, got, c.want)
		}
	}
}
