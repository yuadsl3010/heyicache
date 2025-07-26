package heyicache

import (
	"bytes"
	"errors"
	"io"
)

var ErrOutOfRange = errors.New("out of range")

type buffer struct {
	index int64
	size  int64
	used  int32
	data  []byte
}

func NewBuffer(size int64) *buffer {
	return &buffer{
		index: 0,
		size:  size,
		used:  0,
		data:  make([]byte, size),
	}
}

func (buf *buffer) overflow(p []byte, off int64) bool {
	return off+int64(len(p)) > buf.index || off < 0
}

func (buf *buffer) Alloc(length int64) []byte {
	bs := buf.data[buf.index : buf.index+length]
	buf.index += length
	return bs
}

func (buf *buffer) Slice(off, length int64) []byte {
	return buf.data[off : off+length]
}

func (buf *buffer) WriteAt(p []byte, off int64) (int, error) {
	n := 0
	var err error
	if buf.overflow(p, off) {
		err = ErrOutOfRange
		return n, err
	}
	n = copy(buf.data[off:off+int64(len(p))], p)
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (buf *buffer) EqualAt(p []byte, off int64) bool {
	if buf.overflow(p, off) {
		return false
	}
	return bytes.Equal(p, buf.data[off:off+int64(len(p))])
}
