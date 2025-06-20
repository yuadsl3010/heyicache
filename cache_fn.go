package heyicache

import (
	"unsafe"
)

func HeyiCacheFnSetString(src string, bs []byte) (string, int32) {
	if len(src) == 0 {
		return "", 0
	}

	srcBytes := *(*[]byte)(unsafe.Pointer(&src))
	size := int32(len(srcBytes))
	copy(bs, srcBytes)
	bbs := bs[:size]
	return *(*string)(unsafe.Pointer(&bbs)), size
}

func HeyiCacheFnSizeString(src string) int32 {
	return int32(len(src))
}

var (
	HeyiCacheFnStructSizebool       = int(unsafe.Sizeof(bool(false)))
	HeyiCacheFnStructSizeint        = int(unsafe.Sizeof(int(0)))
	HeyiCacheFnStructSizeint8       = int(unsafe.Sizeof(int8(0)))
	HeyiCacheFnStructSizeint16      = int(unsafe.Sizeof(int16(0)))
	HeyiCacheFnStructSizeint32      = int(unsafe.Sizeof(int32(0)))
	HeyiCacheFnStructSizeint64      = int(unsafe.Sizeof(int64(0)))
	HeyiCacheFnStructSizeuint       = int(unsafe.Sizeof(uint(0)))
	HeyiCacheFnStructSizeuint8      = int(unsafe.Sizeof(uint8(0)))
	HeyiCacheFnStructSizeuint16     = int(unsafe.Sizeof(uint16(0)))
	HeyiCacheFnStructSizeuint32     = int(unsafe.Sizeof(uint32(0)))
	HeyiCacheFnStructSizeuint64     = int(unsafe.Sizeof(uint64(0)))
	HeyiCacheFnStructSizefloat32    = int(unsafe.Sizeof(float32(0)))
	HeyiCacheFnStructSizefloat64    = int(unsafe.Sizeof(float64(0)))
	HeyiCacheFnStructSizecomplex64  = int(unsafe.Sizeof(complex64(0)))
	HeyiCacheFnStructSizecomplex128 = int(unsafe.Sizeof(complex128(0)))
	HeyiCacheFnStructSizeptr        = int(unsafe.Sizeof(uintptr(0))) // Generic pointer size, usually 8 bytes (64-bit system) or 4 bytes (32-bit system)
	HeyiCacheFnStructSizestring     = int(unsafe.Sizeof(string(""))) // The size of a string is dynamic, this is just a placeholder
)

func HeyiCacheFnSetSlice[T any](src []T, bs []byte, lenT int) ([]T, int32) {
	count := len(src)
	if count == 0 {
		return nil, 0
	}

	size := int32(count * lenT)
	srcBytes := unsafe.Slice((*byte)(unsafe.Pointer(&src[0])), size)
	copy(bs, srcBytes)
	dst := unsafe.Slice((*T)(unsafe.Pointer(&bs[0])), count)
	return dst, size
}

func HeyiCacheFnSizeSlice[T any](src []T, lenT int) int32 {
	return int32(len(src)) * int32(lenT)
}
