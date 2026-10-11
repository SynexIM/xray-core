package conf

import "testing"

func TestTCPFingerprintRejectsConflictingSocketState(t *testing.T) {
	for _, c := range []SocketConfig{
		{TCPFingerprint: "unknown"},
		{TCPFingerprint: "windows", Mark: 123},
		{TCPFingerprint: "windows", TCPWindowClamp: 65535},
	} {
		if _, err := c.Build(); err == nil {
			t.Fatal("accepted conflicting or unknown TCP template")
		}
	}
	c := &SocketConfig{TCPFingerprint: "windows"}
	got, err := c.Build()
	if err != nil || got.TcpWindowClamp != 65535<<8 || got.Mark != TCPFingerprintMarkBase|1 {
		t.Fatalf("kernel plan: %v %v", got, err)
	}
	if c.Mark != 0 || c.TCPWindowClamp != 0 {
		t.Fatal("build mutated input")
	}
	plain, err := (&SocketConfig{TCPFingerprint: "none", Mark: 123, TCPWindowClamp: 65535}).Build()
	if err != nil || plain.Mark != 123 || plain.TcpWindowClamp != 65535 {
		t.Fatal("disabled template changed native socket settings")
	}
}
