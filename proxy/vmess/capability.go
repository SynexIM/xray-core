package vmess

import (
	"fmt"
	"github.com/xtls/xray-core/app/capability"
)

const MaxUsersPerInbound = 20000

type UserCapacityError struct{ Maximum int }

func (err *UserCapacityError) Error() string {
	return fmt.Sprintf("vmess.maxUsersPerInbound: 最多 %d 个身份", err.Maximum)
}
func init() { capability.RegisterLimit("vmess.maxUsersPerInbound", MaxUsersPerInbound) }
