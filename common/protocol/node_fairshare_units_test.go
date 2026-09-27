package protocol

import "testing"

// 这个文件只干一件事：把「同一个 `_bps` 后缀，一边比特一边字节，差 8 倍」这个事实
// 钉死，防止有人后来「顺手统一成同一单位」（FR-079d）。
//
//	MemoryUser.BandwidthBps / CommittedBps      比特/秒  ← 业务单位，面板 Mbps × 1e6
//	FairShareService.avail_bps / *_floor_bps    字节/秒  ← 限速器单位，原样进调度器
//
// 换算只允许发生在一个地方：bitsPerSecondToRuntimeBytesPerSecond。
// 新增速率字段一律带 `_bit_per_sec` / `_byte_per_sec` 后缀，不许再用裸 `_bps`。

// 用户侧是比特/秒：进限速器要除 8。
func TestUserBandwidthIsBitsPerSecond(t *testing.T) {
	if got := bitsPerSecondToRuntimeBytesPerSecond(8_000_000); got != 1_000_000 {
		t.Fatalf("8 Mbit/s 应该是 1,000,000 B/s，got %d —— 换算被人改了", got)
	}
	// 向上取整：宁可多给 1 字节，也不要把 1 bit/s 变成 0（= 事实断连）。
	if got := bitsPerSecondToRuntimeBytesPerSecond(1); got != 1 {
		t.Fatalf("want 1, got %d", got)
	}
	if got := bitsPerSecondToRuntimeBytesPerSecond(0); got != 0 {
		t.Fatalf("0 必须还是 0（= 不限速），got %d", got)
	}

	u := &MemoryUser{Email: "x", BandwidthBps: 160_000_000} // 160 Mbit/s
	if got := fairOwnLimitBytesPerSecond(u); got != 20_000_000 {
		t.Fatalf("160 Mbit/s 天花板应是 20,000,000 B/s，got %d", got)
	}
}

// 节点侧是字节/秒：一次都不除 8。
func TestNodeRootCapIsBytesPerSecond(t *testing.T) {
	s := newSched(0)
	s.SetNodeBandwidth(60_000_000) // 480 Mbps 的线，node-agent 已折算成字节/秒
	if got := s.RootCapBytePerSec(); got != 60_000_000 {
		t.Fatalf("root_cap 必须原样存字节/秒，got %d —— 谁在这里除了 8，节点会被掐到 1/8", got)
	}
	s.SetFloors(62_500, 16_384)
	if got := s.softFloorBytePerSec.Load(); got != 62_500 {
		t.Fatalf("软地板必须原样存字节/秒，got %d", got)
	}
	if got := s.hardFloorBytePerSec.Load(); got != 16_384 {
		t.Fatalf("硬地板必须原样存字节/秒，got %d", got)
	}
}

// 两边一起看：同一条线路的同一个数字，用户侧填 8x、节点侧填 x。
// 谁要是把两边「统一」了，这条会红。
func TestBitsAndBytesDifferByEightAcrossTheTwoSurfaces(t *testing.T) {
	const mbps = 480
	userSide := uint64(mbps) * 1_000_000     // User.bandwidth_bps：比特/秒
	nodeSide := uint64(mbps) * 1_000_000 / 8 // avail_bps：字节/秒
	if userSide/nodeSide != 8 {
		t.Fatal("测试自己写错了")
	}

	u := &MemoryUser{Email: "x", BandwidthBps: userSide}
	s := newSched(0)
	s.SetNodeBandwidth(nodeSide)

	if fairOwnLimitBytesPerSecond(u) != s.RootCapBytePerSec() {
		t.Fatalf("同一条 %d Mbps 的线，用户侧算出 %d B/s，节点侧是 %d B/s —— 两边单位不一致了",
			mbps, fairOwnLimitBytesPerSecond(u), s.RootCapBytePerSec())
	}
}
