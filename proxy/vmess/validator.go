package vmess

import (
	"crypto/hmac"
	"crypto/sha256"
	"hash/crc64"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/dice"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/proxy/vmess/aead"
)

// TimedUserValidator is a user Validator based on time.
type TimedUserValidator struct {
	sync.RWMutex
	users []*protocol.MemoryUser
	index map[string]int

	behaviorSeed  uint64
	behaviorFused bool

	authentication    authenticationHistogram
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

	account, ok := u.Account.(*MemoryAccount)
	if !ok {
		return errors.New("account type is incorrect")
	}
	if len(v.users) >= MaxUsersPerInbound {
		return &UserCapacityError{Maximum: MaxUsersPerInbound}
	}
	if u.Email != "" {
		if _, exists := v.index[strings.ToLower(u.Email)]; exists {
			return errors.New("User ", u.Email, " already exists.")
		}
	}
	v.addLocked(u, account)
	return nil
}
func (v *TimedUserValidator) addLocked(u *protocol.MemoryUser, account *MemoryAccount) {
	if u.Email != "" {
		if v.index == nil {
			v.index = make(map[string]int)
		}
		v.index[strings.ToLower(u.Email)] = len(v.users)
	}
	v.users = append(v.users, u)

	if !v.behaviorFused {
		hashkdf := hmac.New(sha256.New, []byte("VMESSBSKDF"))
		hashkdf.Write(account.ID.Bytes())
		v.behaviorSeed = crc64.Update(v.behaviorSeed, crc64.MakeTable(crc64.ECMA), hashkdf.Sum(nil))
	}

	var cmdkeyfl [16]byte
	copy(cmdkeyfl[:], account.ID.CmdKey())
	v.aeadDecoderHolder.AddUser(cmdkeyfl, u)

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

func (v *TimedUserValidator) GetAEAD(userHash []byte, sourceIP ...string) (*protocol.MemoryUser, bool, error) {
	start := time.Now()
	defer func() { v.authentication.observe(time.Since(start)) }()
	v.RLock()
	defer v.RUnlock()

	var userHashFL [16]byte
	copy(userHashFL[:], userHash)

	userd, err := v.aeadDecoderHolder.Match(userHashFL, sourceIP...)
	if err != nil {
		return nil, false, err
	}
	return userd.(*protocol.MemoryUser), true, nil
}

func (v *TimedUserValidator) Remove(email string) bool {
	v.Lock()
	defer v.Unlock()

	email = strings.ToLower(email)
	_, ok := v.index[email]
	if !ok {
		return false
	}
	v.removeLocked(email)
	return true
}
func (v *TimedUserValidator) removeLocked(email string) {
	idx, ok := v.index[email]
	if !ok {
		return
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
