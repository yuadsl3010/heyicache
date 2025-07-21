package heyicache

import (
	"bytes"
	"errors"
	"io"
)

var ErrOutOfRange = errors.New("out of range")

type buffer struct {
	index int32
	size  int32
	data  []byte
}

func NewBuffer(size int32) *buffer {
	return &buffer{
		index: 0,
		size:  size,
		data:  make([]byte, size),
	}
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
