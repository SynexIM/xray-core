package protocol

import "testing"

func TestRetiredRuntimeSessionsCannotReviveAndHotUpdatesRetainLifetime(t *testing.T) {
	first := &MemoryUser{Email: "demo-a"}
	stopped := 0
	release, ok := first.TrackRuntimeSession(func() { stopped++ })
	if !ok {
		t.Fatal("valid session rejected")
	}
	defer release()
	next := &MemoryUser{Email: "demo-a"}
	next.ShareRuntimeSessions(first)
	next.RevokeRuntimeSessions()
	if stopped != 1 {
		t.Fatal("hot update lost an established session")
	}
	if _, ok = first.TrackRuntimeSession(func() { stopped++ }); ok {
		t.Fatal("late authenticated retired identity revived")
	}
	fresh := &MemoryUser{Email: "demo-a"}
	if _, ok = fresh.TrackRuntimeSession(func() {}); !ok {
		t.Fatal("a new valid identity inherited revocation")
	}
}
