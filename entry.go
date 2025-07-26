package heyicache

import "errors"

const HASH_ENTRY_SIZE = 16
const ENTRY_HDR_SIZE int64 = 24

var ErrLargeKey = errors.New("The key is larger than 65535")
var ErrLargeEntry = errors.New("The entry size is larger than 1/1024 of cache size")
var ErrNotFound = errors.New("Entry not found")

type entryPtr struct {
	offset int64  // entry offset in buffer
	hash16 uint16 // entries are ordered by hash16 in a slot.
	keyLen uint16 // used to compare a key
	block  int32
}

// entry header struct in ring buffer, followed by key and value.
type entryHdr struct {
	accessTime uint32
	expireAt   uint32
	keyLen     uint16
	hash16     uint16
	valLen     uint32
	deleted    bool
	slotId     uint8
	_          uint16
	_          uint32
}
