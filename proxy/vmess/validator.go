package vmess

import (
	"crypto/hmac"
	"crypto/sha256"
	"hash/crc64"
	"strings"
	"sync"

	"github.com/xtls/xray-core/common/dice"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/proxy/vmess/aead"
)

// TimedUserValidator is a user Validator based on time.
//
// index 是 strings.ToLower(email) → users 下标：Remove 原来线性扫 email，
// 5 万实例/单节点组下管理路径就是 O(N)。认证路径（GetAEAD）本来就是 O(1)
// 哈希查表，没动。
type TimedUserValidator struct {
	sync.RWMutex
	users []*protocol.MemoryUser
	index map[string]int

	behaviorSeed  uint64
	behaviorFused bool

	aeadDecoderHolder *aead.AuthIDDecoderHolder
}

// NewTimedUserValidator creates a new TimedUserValidator.
func NewTimedUserValidator() *TimedUserValidator {
	tuv := &TimedUserValidator{
		users:             make([]*protocol.MemoryUser, 0, 16),
		index:             make(map[string]int, 16),
		aeadDecoderHolder: aead.NewAuthIDDecoderHolder(),
	}
	return tuv
}

func (v *TimedUserValidator) Add(u *protocol.MemoryUser) error {
	v.Lock()
	defer v.Unlock()

	if u.Email != "" {
		if v.index == nil {
			v.index = make(map[string]int)
		}
		v.index[strings.ToLower(u.Email)] = len(v.users)
	}
	v.users = append(v.users, u)

	account, ok := u.Account.(*MemoryAccount)
	if !ok {
		return errors.New("account type is incorrect")
	}
	if !v.behaviorFused {
		hashkdf := hmac.New(sha256.New, []byte("VMESSBSKDF"))
		hashkdf.Write(account.ID.Bytes())
		v.behaviorSeed = crc64.Update(v.behaviorSeed, crc64.MakeTable(crc64.ECMA), hashkdf.Sum(nil))
	}

	var cmdkeyfl [16]byte
	copy(cmdkeyfl[:], account.ID.CmdKey())
	v.aeadDecoderHolder.AddUser(cmdkeyfl, u)

	return nil
}

func (v *TimedUserValidator) GetUsers() []*protocol.MemoryUser {
	v.Lock()
	defer v.Unlock()
	dst := make([]*protocol.MemoryUser, len(v.users))
	copy(dst, v.users)
	return dst
}

func (v *TimedUserValidator) GetCount() int64 {
	v.Lock()
	defer v.Unlock()
	return int64(len(v.users))
}

func (v *TimedUserValidator) GetAEAD(userHash []byte) (*protocol.MemoryUser, bool, error) {
	v.RLock()
	defer v.RUnlock()

	var userHashFL [16]byte
	copy(userHashFL[:], userHash)

	userd, err := v.aeadDecoderHolder.Match(userHashFL)
	if err != nil {
		return nil, false, err
	}
	return userd.(*protocol.MemoryUser), true, nil
}

func (v *TimedUserValidator) Remove(email string) bool {
	v.Lock()
	defer v.Unlock()

	email = strings.ToLower(email)
	idx, ok := v.index[email]
	if !ok {
		return false
	}
	var cmdkeyfl [16]byte
	copy(cmdkeyfl[:], v.users[idx].Account.(*MemoryAccount).ID.CmdKey())
	v.aeadDecoderHolder.RemoveUser(cmdkeyfl)

	last := len(v.users) - 1
	delete(v.index, email)
	if idx != last {
		v.users[idx] = v.users[last]
		v.index[strings.ToLower(v.users[idx].Email)] = idx
	}
	v.users[last] = nil
	v.users = v.users[:last]

	return true
}

func (v *TimedUserValidator) GetBehaviorSeed() uint64 {
	v.Lock()
	defer v.Unlock()

	v.behaviorFused = true
	if v.behaviorSeed == 0 {
		v.behaviorSeed = dice.RollUint64()
	}
	return v.behaviorSeed
}

var ErrNotFound = errors.New("Not Found")

var ErrTainted = errors.New("ErrTainted")
