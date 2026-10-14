package vmess

import (
	"errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/uuid"
	"testing"
)

func TestCapacityRejectsWholeBatchWithoutMutation(t *testing.T) {
	validator := vmessValidatorWith(t, MaxUsersPerInbound-1)
	users := []*protocol.MemoryUser{{Email: "new-first", Account: &MemoryAccount{ID: protocol.NewID(uuid.New())}}, {Email: "new-second", Account: &MemoryAccount{ID: protocol.NewID(uuid.New())}}}
	var capacity *UserCapacityError
	if err := validator.AddUsers(users); !errors.As(err, &capacity) || capacity.Maximum != MaxUsersPerInbound {
		t.Fatalf("上限错误类型: %v", err)
	}
	if validator.GetCount() != MaxUsersPerInbound-1 {
		t.Fatal("拒绝的批次留下身份")
	}
	if err := validator.Add(users[0]); err != nil {
		t.Fatal(err)
	}
	if err := validator.Add(users[1]); !errors.As(err, &capacity) {
		t.Fatalf("单条没有守住上限: %v", err)
	}
	if err := validator.RemoveUsers([]string{"new-first", "missing"}); err == nil {
		t.Fatal("批量删除没检查不存在的身份")
	}
	if !validator.Remove("new-first") {
		t.Fatal("失败删除留下部分变更")
	}
}
