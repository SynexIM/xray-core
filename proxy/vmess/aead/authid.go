package aead

import (
	"bytes"
	"container/list"
	"crypto/aes"
	"crypto/cipher"
	rand3 "crypto/rand"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync/atomic"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/antireplay"
)

var (
	ErrNotFound     = errors.New("user do not exist")
	ErrNeagtiveTime = errors.New("timestamp is negative")
	ErrInvalidTime  = errors.New("invalid timestamp, perhaps unsynchronized time")
	ErrReplay       = errors.New("replayed request")
)

func CreateAuthID(cmdKey []byte, time int64) [16]byte {
	buf := bytes.NewBuffer(nil)
	common.Must(binary.Write(buf, binary.BigEndian, time))
	var zero uint32
	common.Must2(io.CopyN(buf, rand3.Reader, 4))
	zero = crc32.ChecksumIEEE(buf.Bytes())
	common.Must(binary.Write(buf, binary.BigEndian, zero))
	aesBlock := NewCipherFromKey(cmdKey)
	if buf.Len() != 16 {
		panic("Size unexpected")
	}
	var result [16]byte
	aesBlock.Encrypt(result[:], buf.Bytes())
	return result
}

func NewCipherFromKey(cmdKey []byte) cipher.Block {
	aesBlock, err := aes.NewCipher(KDF16(cmdKey, KDFSaltConstAuthIDEncryptionKey))
	if err != nil {
		panic(err)
	}
	return aesBlock
}

type AuthIDDecoder struct {
	s cipher.Block
}

func NewAuthIDDecoder(cmdKey []byte) *AuthIDDecoder {
	return &AuthIDDecoder{NewCipherFromKey(cmdKey)}
}

func (aidd *AuthIDDecoder) Decode(data [16]byte) (int64, uint32, int32, []byte) {
	aidd.s.Decrypt(data[:], data[:])
	var t int64
	var zero uint32
	var rand int32
	reader := bytes.NewReader(data[:])
	common.Must(binary.Read(reader, binary.BigEndian, &t))
	common.Must(binary.Read(reader, binary.BigEndian, &rand))
	common.Must(binary.Read(reader, binary.BigEndian, &zero))
	return t, zero, rand, data[:]
}

func NewAuthIDDecoderHolder() *AuthIDDecoderHolder {
	return &AuthIDDecoderHolder{index: newDecoderIndex(), filter: antireplay.NewMapFilter[[16]byte](120)}
}

type AuthIDDecoderHolder struct {
	index  *decoderIndex
	filter *antireplay.ReplayFilter[[16]byte]
}

type AuthIDDecoderItem struct {
	dec      *AuthIDDecoder
	ticket   interface{}
	segment  byte
	position *list.Element
	removed  atomic.Bool
}

func NewAuthIDDecoderItem(key [16]byte, ticket interface{}) *AuthIDDecoderItem {
	return &AuthIDDecoderItem{
		dec:    NewAuthIDDecoder(key[:]),
		ticket: ticket,
	}
}

func (a *AuthIDDecoderHolder) AddUser(key [16]byte, ticket interface{}) { a.index.add(key, ticket) }
func (a *AuthIDDecoderHolder) RemoveUser(key [16]byte)                  { a.index.remove(key) }
func (a *AuthIDDecoderHolder) Match(authID [16]byte, sourceIP ...string) (interface{}, error) {
	source := ""
	if len(sourceIP) > 0 {
		source = sourceIP[0]
	}
	return a.matchSource(authID, source)
}
