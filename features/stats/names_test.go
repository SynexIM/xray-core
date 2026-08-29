package stats

import "testing"

func TestActiveConnectionCounterNamesAreStable(t *testing.T) {
	if got, want := ActiveConnectionCounterName("mixed-in"), "inbound>>>mixed-in>>>connections>>>active"; got != want {
		t.Fatalf("ActiveConnectionCounterName = %q, want %q", got, want)
	}
	if got, want := ActiveUserConnectionCounterName("client-uid"), "user>>>client-uid>>>connections>>>active"; got != want {
		t.Fatalf("ActiveUserConnectionCounterName = %q, want %q", got, want)
	}
}
