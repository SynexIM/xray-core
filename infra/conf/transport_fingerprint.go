package conf

import (
	"fmt"
	"strconv"

	"github.com/xtls/xray-core/transport/internet"
)

// A dedicated range avoids interpreting ordinary policy-routing marks as OS
// templates. The tc program recognizes exactly these three values.
const TCPFingerprintMarkBase int32 = 0x51460000

type TCPFingerprintProfile struct {
	Name               string
	Mark               int32
	WindowScale        uint32
	ReceiveBufferBytes int
}

// A detached catalogue is shared by socket compilation and agent capability
// checks. A node cannot advertise a profile whose kernel buffer limits reject it.
func TCPFingerprintProfiles() []TCPFingerprintProfile {
	return []TCPFingerprintProfile{
		{Name: "windows", Mark: TCPFingerprintMarkBase | 1, WindowScale: 8},
		{Name: "macos", Mark: TCPFingerprintMarkBase | 2, WindowScale: 6, ReceiveBufferBytes: 4 << 20},
		{Name: "android", Mark: TCPFingerprintMarkBase | 3, WindowScale: 7, ReceiveBufferBytes: 5 << 20},
	}
}

func TCPFingerprintMark(name string) (int32, error) {
	if name == "" || name == "none" {
		return 0, nil
	}
	for _, p := range TCPFingerprintProfiles() {
		if p.Name == name {
			return p.Mark, nil
		}
	}
	return 0, fmt.Errorf("tcpFingerprint: choose none, windows, macos or android")
}

func (c *SocketConfig) applyFingerprint(out *internet.SocketConfig) error {
	mark, err := TCPFingerprintMark(c.TCPFingerprint)
	if err != nil {
		return err
	}
	if mark == 0 {
		return nil
	}
	if c.Mark != 0 || c.TCPWindowClamp != 0 || len(c.CustomSockopt) != 0 || c.TcpMptcp || out.Tfo > 0 {
		return fmt.Errorf("tcpFingerprint cannot combine with mark, tcpWindowClamp, customSockopt, MPTCP or TCP Fast Open")
	}
	// Negotiate the scale in the socket itself. Rewriting the SYN's scale would
	// corrupt the interpretation of every subsequent advertised window.
	for _, p := range TCPFingerprintProfiles() {
		if p.Mark != mark {
			continue
		}
		out.Mark, out.TcpWindowClamp = mark, int32(65535<<p.WindowScale)
		if p.ReceiveBufferBytes != 0 {
			out.CustomSockopt = append(out.CustomSockopt, &internet.CustomSockopt{
				System: "linux", Network: "tcp", Level: "1", Opt: "8", Type: "int", Value: strconv.Itoa(p.ReceiveBufferBytes),
			})
		}
	}
	return nil
}
