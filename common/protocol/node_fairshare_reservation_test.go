package protocol

import "testing"

func directionBacklogged(
	member *fairMember,
	direction fairDirection,
	bytes uint64,
) {
	state := directionState(member, direction)
	state.bytes.Add(bytes)
	state.blocked.Add(1)
}

func directionSatisfied(
	member *fairMember,
	direction fairDirection,
	bytes uint64,
) {
	directionState(member, direction).bytes.Add(bytes)
}

func TestDirectionalReservationIsAggregatePriorityAndUnusedCapacityFlowsBack(
	t *testing.T,
) {
	const root = 100_000_000
	scheduler := newSched(root)
	scheduler.SetClassPolicies([]*ClassPolicy{{
		Name:                       "reservation-live",
		Weight:                     1,
		UploadReservedBytePerSec:   60_000_000,
		DownloadReservedBytePerSec: 20_000_000,
		MemberIDs:                  []string{"reserved-a", "reserved-b"},
	}})
	reserved := []*fairMember{
		memberIn(scheduler, "reserved-a", "standard-sq", 0),
		memberIn(scheduler, "reserved-b", "standard-sq", 0),
	}
	bestEffort := []*fairMember{
		memberFor(scheduler, "best-a", 0),
		memberFor(scheduler, "best-b", 0),
	}
	for _, member := range append(
		append([]*fairMember(nil), reserved...), bestEffort...,
	) {
		directionBacklogged(member, fairUpload, 1<<20)
		directionBacklogged(member, fairDownload, 1<<20)
	}
	scheduler.recompute()

	var reservedUpload, reservedDownload uint64
	for _, member := range reserved {
		reservedUpload += uint64(member.upLimiter.Limit())
		reservedDownload += uint64(member.downLimiter.Limit())
		if got := uint64(member.upLimiter.Limit()); got >= 60_000_000 {
			t.Fatalf(
				"60MB/s aggregate reservation became a per-member floor: %d",
				got,
			)
		}
	}
	if reservedUpload < 60_000_000 {
		t.Fatalf("upload reservation not honored: %d", reservedUpload)
	}
	if reservedDownload < 20_000_000 {
		t.Fatalf("download reservation not honored: %d", reservedDownload)
	}
	if reservedUpload <= reservedDownload {
		t.Fatalf(
			"directional reservations collapsed: upload=%d download=%d",
			reservedUpload, reservedDownload,
		)
	}
	for _, direction := range []fairDirection{fairUpload, fairDownload} {
		var total uint64
		for _, member := range scheduler.members {
			total += directionState(member, direction).allocation
		}
		if total > root {
			t.Fatalf("%v allocation=%d exceeds root=%d", direction, total, root)
		}
	}

	// Reservation members now ask for very little. Their unused admitted floor
	// must not sit idle while best-effort members are backlogged.
	for _, member := range reserved {
		directionSatisfied(member, fairUpload, 8_000)
		directionSatisfied(member, fairDownload, 8_000)
	}
	for _, member := range bestEffort {
		directionBacklogged(member, fairUpload, 1<<20)
		directionBacklogged(member, fairDownload, 1<<20)
	}
	scheduler.recompute()
	for _, member := range bestEffort {
		if got := uint64(member.upLimiter.Limit()); got < 40_000_000 {
			t.Fatalf("unused reservation did not flow back: best effort=%d", got)
		}
	}
}

func TestRemovingReservationReturnsMembersToOrdinaryFairShare(t *testing.T) {
	const root = 80_000_000
	scheduler := newSched(root)
	scheduler.SetClassPolicies([]*ClassPolicy{{
		Name: "reservation-live", Weight: 1,
		UploadReservedBytePerSec: 60_000_000,
		MemberIDs:                []string{"reserved"},
	}})
	reserved := memberIn(scheduler, "reserved", "standard-sq", 0)
	bestEffort := memberFor(scheduler, "best", 0)
	if !scheduler.HasReservation("reserved") ||
		scheduler.HasReservation("best") {
		t.Fatal("reservation membership index is wrong")
	}
	for _, member := range []*fairMember{reserved, bestEffort} {
		directionBacklogged(member, fairUpload, 1<<20)
	}
	scheduler.recompute()
	if uint64(reserved.upLimiter.Limit()) <=
		uint64(bestEffort.upLimiter.Limit()) {
		t.Fatalf(
			"reservation had no priority: reserved=%v best=%v",
			reserved.upLimiter.Limit(), bestEffort.upLimiter.Limit(),
		)
	}

	scheduler.SetClassPolicies(nil)
	if scheduler.HasReservation("reserved") {
		t.Fatal("reservation membership survived policy removal")
	}
	for _, member := range []*fairMember{reserved, bestEffort} {
		directionBacklogged(member, fairUpload, 1<<20)
	}
	scheduler.recompute()
	if reserved.upLimiter.Limit() != bestEffort.upLimiter.Limit() ||
		uint64(reserved.upLimiter.Limit()) != root/2 {
		t.Fatalf(
			"reservation deletion left priority behind: reserved=%v best=%v",
			reserved.upLimiter.Limit(), bestEffort.upLimiter.Limit(),
		)
	}
}
