package flowlog

import (
	"fmt"
	"os"
	"path/filepath"
)

// rotator 是按大小轮转的追加写文件：path 写满 maxSize 就依次挪成 path.1 … path.N，最旧的删掉。
// 只由写协程调用，不加锁。
type rotator struct {
	path    string
	maxSize int64
	keep    int
	f       *os.File
	size    int64
}

func newRotator(path string, maxSize int64, keep int) (*rotator, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	r := &rotator{path: path, maxSize: maxSize, keep: keep}
	return r, r.open()
}

func (r *rotator) open() error {
	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	r.f, r.size = f, st.Size()
	return nil
}

func (r *rotator) Write(p []byte) error {
	if r.size+int64(len(p)) > r.maxSize && r.size > 0 {
		if err := r.rotate(); err != nil {
			return err
		}
	}
	n, err := r.f.Write(p)
	r.size += int64(n)
	return err
}

func (r *rotator) rotate() error {
	if err := r.f.Close(); err != nil {
		return err
	}
	_ = os.Remove(fmt.Sprintf("%s.%d", r.path, r.keep)) // 最旧的那个可能本来就不存在
	for i := r.keep - 1; i >= 1; i-- {
		_ = os.Rename(fmt.Sprintf("%s.%d", r.path, i), fmt.Sprintf("%s.%d", r.path, i+1))
	}
	if err := os.Rename(r.path, r.path+".1"); err != nil {
		return err
	}
	return r.open()
}

func (r *rotator) Close() error { return r.f.Close() }
