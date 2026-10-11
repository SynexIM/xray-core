package shadowsocks_2022

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/utils"
)

// Measures the actual upstream user tables, not a substitute map implementation.
func BenchmarkUserChurn(b *testing.B) {
	for _, n := range []int{1000, 100000, 1000000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			method, err := GetCipherMethod("2022-blake3-aes-128-gcm")
			if err != nil {
				b.Fatal(err)
			}
			i := &MultiUserInbound{method: method,
				usersByHash:  utils.NewTypedSyncMap[[AESBlockSize]byte, *protocol.MemoryUser](),
				usersByEmail: utils.NewTypedSyncMap[string, *protocol.MemoryUser](),
			}
			ctx := context.Background()
			for j := 0; j < n; j++ {
				key := make([]byte, 16)
				binary.LittleEndian.PutUint64(key, uint64(j))
				if err := i.AddUser(ctx, &protocol.MemoryUser{Email: fmt.Sprint(j), Account: &MemoryAccount{Key: key}}); err != nil {
					b.Fatal(err)
				}
			}
			u := i.GetUser(ctx, "0")
			b.ResetTimer()
			for b.Loop() {
				if err := i.RemoveUser(ctx, "0"); err != nil {
					b.Fatal(err)
				}
				if err := i.AddUser(ctx, u); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
