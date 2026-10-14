package aead

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"
)

func TestSourceCacheSharedAddressAndRevocation(t *testing.T) {
	holder := NewAuthIDDecoderHolder()
	var first, second [16]byte
	first[0] = 1
	second[0] = 2
	holder.AddUser(first, "first")
	holder.AddUser(second, "second")
	for _, key := range [][16]byte{first, second, first, first} {
		authID := CreateAuthID(key[:], time.Now().Unix())
		value, err := holder.Match(authID, "192.0.2.1")
		expected := "first"
		if key == second {
			expected = "second"
		}
		if err != nil || value != expected {
			t.Fatalf("认证失败: %v %v", value, err)
		}
		if _, err := holder.Match(authID, "192.0.2.1"); err != ErrReplay {
			t.Fatalf("缓存绕过重放过滤: %v", err)
		}
	}
	holder.RemoveUser(first)
	authID := CreateAuthID(first[:], time.Now().Unix())
	if _, err := holder.Match(authID, "192.0.2.1"); err != ErrNotFound {
		t.Fatalf("已删除身份仍可认证: %v", err)
	}
	old := CreateAuthID(second[:], time.Now().Add(-3*time.Minute).Unix())
	if _, err := holder.Match(old, "192.0.2.1"); err != ErrInvalidTime {
		t.Fatalf("缓存绕过时间校验: %v", err)
	}
}
func TestSegmentedRecentIndexConcurrentMutations(t *testing.T) {
	holder := NewAuthIDDecoderHolder()
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			var key [16]byte
			binary.BigEndian.PutUint64(key[:8], uint64(worker+1))
			key[0] = byte(worker)
			for iteration := 0; iteration < 100; iteration++ {
				holder.AddUser(key, worker)
				authID := CreateAuthID(key[:], time.Now().Unix())
				value, err := holder.Match(authID, "192.0.2.1")
				if err != nil || value != worker {
					t.Errorf("并发认证失败: %v %v", value, err)
					return
				}
				holder.RemoveUser(key)
			}
		}(worker)
	}
	wait.Wait()
}
