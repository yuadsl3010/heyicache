package heyicache

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
)

var ErrOutOfRange = errors.New("out of range")

type buffer struct {
	index int32
	size  int32
	used  int64
	data  []byte
}

func NewBuffer(size int32, alloc bool, shuffleRatio float32) *buffer {
	buf := &buffer{
		index: 0,
		size:  size,
		used:  0,
		data:  nil,
	}

	if alloc {
		buf.data = make([]byte, size)
		if shuffleRatio > 0 {
			buf.index = int32(float32(size) * (rand.Float32() * shuffleRatio))
		}
	}

	return buf
}

func (buf *buffer) Clear() {
	buf.index = 0
	buf.used = 0
	buf.data = nil
}

func (buf *buffer) ReAlloc() {
	buf.index = 0
	buf.used = 0
	buf.data = make([]byte, buf.size)
}

func (buf *buffer) overflow(p []byte, off int32) bool {
	return off+int32(len(p)) > buf.index || off < 0
}

func (buf *buffer) Alloc(length int32) []byte {
	bs := buf.data[buf.index : buf.index+length]
	buf.index += length
	return bs
}

func (buf *buffer) Slice(off, length int32) []byte {
	return buf.data[off : off+length]
}

func (buf *buffer) ReadAt(p []byte, off int32) (int, error) {
	n := 0
	var err error
	if buf.overflow(p, off) {
		err = ErrOutOfRange
		return n, err
	}
	n = copy(p, buf.data[off:off+int32(len(p))])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (buf *buffer) WriteAt(p []byte, off int32) (int, error) {
	n := 0
	var err error
	if buf.overflow(p, off) {
		err = ErrOutOfRange
		return n, err
	}
	n = copy(buf.data[off:off+int32(len(p))], p)
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (buf *buffer) EqualAt(p []byte, off int32) bool {
	if buf.overflow(p, off) {
		return false
	}
	return bytes.Equal(p, buf.data[off:off+int32(len(p))])
}
