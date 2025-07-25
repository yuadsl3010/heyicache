package heyicache

import (
	"errors"
	"sync/atomic"
	"time"
	"unsafe"
)

var ErrSegmentFull = errors.New("segment is full, please wait for automatic eviction")
var ErrSegmentBusy = errors.New("segment is busy, please check if the beyond ratio is set too small")
var ErrSegmentUnlucky = errors.New("segment is unlucky, please retry")
var ErrValueTooBig = errors.New("value is too big, please use smaller value or increase cache size")
var ErrSegmentCleaning = errors.New("segment has been expanded, re-allocate space, please retry")
var ErrDuplicateWrite = errors.New("write interval less than MinWriteSecondsForSameKey, no need to write again")
var maxLocateRetry = 3
var sleepLocate = 1 * time.Millisecond // ms

// it's quite different from freecache, cause we don't need to use ring buffer
// once found a segment is full, we will allocate a new segment and release the old one
type segment struct {
	bufs              [blockCount]*buffer
	segId             int32
	curBlock          int32
	nextBlock         int32
	_                 int32
	timer             Timer // Timer giving current time
	entryCount        int64
	evictionNum       int64
	evictionCount     int64
	evictionWaitCount int64
	expireCount       int64
	missCount         int64
	hitCount          int64 // miss + hit = read
	writeCount        int64 // write
	writeErrCount     int64 // write error count
	overwriteCount    int64
	skipWriteCount    int64 // skip write if the entry already exists in very short time
	minWriteInterval  int32
	slotCap           int32            // max number of entry pointers a slot can hold.
	slotsLen          [slotCount]int32 // the length for every slot
	slotsData         []entryPtr
}

func newSegment(bufSize int64, segId int32, minWriteInterval int32, timer Timer) segment {
	seg := segment{
		bufs:             [blockCount]*buffer{},
		segId:            segId,
		timer:            timer,
		slotCap:          1,
		slotsData:        make([]entryPtr, slotCount),
		minWriteInterval: minWriteInterval,
		curBlock:         0,
		nextBlock:        1,
	}

	for i := 0; i < int(blockCount); i++ {
		seg.bufs[i] = NewBuffer(bufSize / int64(blockCount))
	}

	return seg
}

func (seg *segment) getBuffer(ptr *entryPtr) *buffer {
	return seg.bufs[ptr.block]
}

func (seg *segment) getHdr(ptr *entryPtr) *entryHdr {
	return (*entryHdr)(unsafe.Pointer(&seg.getBuffer(ptr).data[ptr.offset]))
}

func (seg *segment) getCurBuffer() *buffer {
	return seg.bufs[seg.curBlock]
}

func (seg *segment) enough(allSize int64) bool {
	return allSize+seg.getCurBuffer().index < seg.getCurBuffer().size
}

//go:inline
func (seg *segment) update(block int32, k int32) {
	seg.bufs[block].used += k
	if seg.nextBlock != block {
		return
	}

	// only clear the next block
	buf := seg.bufs[block]
	if buf.used == 0 {
		// clear the buffer
		offset := int64(0)
		for offset+ENTRY_HDR_SIZE <= buf.index {
			hdr := (*entryHdr)(unsafe.Pointer(&buf.data[offset]))
			seg.delEntryPtrByOffset(hdr.slotId, hdr.hash16, offset)
			atomic.AddInt64(&seg.evictionNum, 1)
			offset = offset + ENTRY_HDR_SIZE + int64(hdr.keyLen) + int64(hdr.valLen)
		}

		atomic.AddInt64(&seg.evictionCount, 1)
		buf.index = 0
		buf.data = make([]byte, buf.size)
	}
}

func (seg *segment) eviction() error {
	if seg.bufs[seg.nextBlock].used > 0 {
		// it's only two cases
		// 1. the speed of generating is too fast: expand the cache size
		// 2. some interfaces getted from Get() but not released by Done(): check the code logic
		// for case 1, I think 3 buffers are enough, just expand the cache size will decrease the write error ratio
		atomic.AddInt64(&seg.evictionWaitCount, 1)
		return ErrSegmentFull
	}

	// clean the next block
	seg.update(seg.nextBlock, 0)
	seg.curBlock = seg.nextBlock
	seg.nextBlock = (seg.curBlock + 1) % blockCount
	return nil
}

func (seg *segment) alloc(key []byte, valueSize int32) ([]byte, int64, error) {
	// param check
	if len(key) > 65535 {
		return nil, 0, ErrLargeKey
	}

	// check buffer size
	allSize := ENTRY_HDR_SIZE + int64(len(key)) + int64(valueSize)
	if !seg.enough(allSize) {
		// not enough space in segment, return error.
		// the caller should try to allocate a new segment.
		err := seg.eviction()
		if err != nil {
			return nil, 0, err
		}

		if !seg.enough(allSize) {
			// still not enough space, return error.
			return nil, 0, ErrValueTooBig
		}
	}

	// direct alloc buffer
	index := seg.getCurBuffer().index
	return seg.getCurBuffer().Alloc(allSize), index, nil
}

func (seg *segment) createHdr(block int32, key []byte, valueSize int32, hashVal uint64, expireSeconds int) (*entryHdr, []byte, error) {
	// check if the key already exists
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if match {
		if seg.minWriteInterval == 0 {
			// the exist memory can not be modified, so we need to delete it
			atomic.AddInt64(&seg.overwriteCount, 1)
			seg.delEntryPtr(slotId, slot, idx)
		} else {
			// need to check if the write interval is too short
			ptr := &slot[idx]
			hdr := seg.getHdr(ptr)
			if seg.timer.Now()-hdr.accessTime <= uint32(seg.minWriteInterval) {
				// the write interval is too short, skip this write
				atomic.AddInt64(&seg.skipWriteCount, 1)
				return nil, nil, ErrDuplicateWrite
			} else {
				// the write interval is long enough, we can overwrite the old entry(actually delete it and rewrite another one)
				atomic.AddInt64(&seg.overwriteCount, 1)
				seg.delEntryPtr(slotId, slot, idx)
			}
		}
	}

	// allocate space in segment
	bs, index, err := seg.alloc(key, valueSize)
	if err != nil {
		atomic.AddInt64(&seg.writeErrCount, 1)
		return nil, nil, err
	}

	if seg.curBlock != block {
		// segment has been expanded, re-allocate space
		atomic.AddInt64(&seg.writeErrCount, 1)
		return nil, nil, ErrSegmentCleaning
	}

	// init a new entry header
	hdr := (*entryHdr)(unsafe.Pointer(&bs[0]))
	// expire time
	now := seg.timer.Now()
	expireAt := uint32(0)
	if expireSeconds > 0 {
		expireAt = now + uint32(expireSeconds)
	}

	// header detail
	hdr.slotId = slotId
	hdr.hash16 = hash16
	hdr.keyLen = uint16(len(key))
	hdr.valLen = uint32(valueSize)
	hdr.valCap = uint32(valueSize)
	hdr.accessTime = now
	hdr.expireAt = expireAt
	hdr.deleted = true // mark as deleted first, then write the key and value

	// insert the node
	seg.insertEntryPtr(slotId, hash16, index, idx, hdr.keyLen)
	atomic.AddInt64(&seg.writeCount, 1)
	return hdr, bs, nil
}

func (seg *segment) write(bs []byte, key []byte, value interface{}, fn HeyiCacheFnIfc) {
	// cache 1. write key
	copy(bs[ENTRY_HDR_SIZE:], key)

	// cache 2. write value
	fn.Set(value, bs[ENTRY_HDR_SIZE+int64(len(key)):], true)
}

func (seg *segment) get(key []byte, fn HeyiCacheFnIfc, hashVal uint64, peek bool) (interface{}, error) {
	hdr, ptr, err := seg.locate(key, hashVal, peek)
	if err != nil {
		return nil, err
	}

	start := ptr.offset + ENTRY_HDR_SIZE + int64(hdr.keyLen)
	bs := seg.getBuffer(ptr).Slice(start, int64(hdr.valLen))
	if !peek {
		atomic.AddInt64(&seg.hitCount, 1)
	}

	return fn.Get(bs), nil
}

func (seg *segment) del(key []byte, hashVal uint64) (affected bool) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		return false
	}
	seg.delEntryPtr(slotId, slot, idx)
	return true
}

func (seg *segment) locate(key []byte, hashVal uint64, peek bool) (*entryHdr, *entryPtr, error) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		if !peek {
			atomic.AddInt64(&seg.missCount, 1)
		}
		return nil, nil, ErrNotFound
	}

	ptr := &slot[idx]
	hdr := seg.getHdr(ptr)
	if hdr.deleted {
		if !peek {
			atomic.AddInt64(&seg.missCount, 1)
		}
		return nil, nil, ErrNotFound
	}

	if !peek {
		now := seg.timer.Now()
		if isExpired(hdr.expireAt, now) {
			seg.delEntryPtr(slotId, slot, idx)
			atomic.AddInt64(&seg.expireCount, 1)
			atomic.AddInt64(&seg.missCount, 1)
			return nil, nil, ErrNotFound
		}
		hdr.accessTime = now
	}
	return hdr, ptr, nil
}

func entryPtrIdx(slot []entryPtr, hash16 uint16) int {
	idx := 0
	high := len(slot)
	for idx < high {
		mid := (idx + high) >> 1
		oldEntry := &slot[mid]
		if oldEntry.hash16 < hash16 {
			idx = mid + 1
		} else {
			high = mid
		}
	}
	return idx
}

func (seg *segment) lookup(slot []entryPtr, hash16 uint16, key []byte) (int, bool) {
	match := false
	idx := entryPtrIdx(slot, hash16)
	for idx < len(slot) {
		ptr := &slot[idx]
		if ptr.hash16 != hash16 {
			break
		}
		match = int(ptr.keyLen) == len(key) && seg.getBuffer(ptr).EqualAt(key, ptr.offset+ENTRY_HDR_SIZE)
		if match {
			return idx, match
		}
		idx++
	}
	return idx, match
}

func (seg *segment) lookupByOff(slot []entryPtr, hash16 uint16, offset int64) (int, bool) {
	match := false
	idx := entryPtrIdx(slot, hash16)
	for idx < len(slot) {
		ptr := &slot[idx]
		if ptr.hash16 != hash16 {
			break
		}
		match = ptr.offset == offset
		if match {
			return idx, match
		}
		idx++
	}
	return idx, match
}

func (seg *segment) expand() {
	newSlotData := make([]entryPtr, seg.slotCap*2*slotCount)
	for i := 0; i < int(slotCount); i++ {
		off := int32(i) * seg.slotCap
		copy(newSlotData[off*2:], seg.slotsData[off:off+seg.slotsLen[i]])
	}
	seg.slotCap *= 2
	seg.slotsData = newSlotData
}

func (seg *segment) insertEntryPtr(slotId uint8, hash16 uint16, offset int64, idx int, keyLen uint16) {
	if seg.slotsLen[slotId] == seg.slotCap {
		seg.expand()
	}
	seg.slotsLen[slotId]++
	atomic.AddInt64(&seg.entryCount, 1)
	slot := seg.getSlot(slotId)
	copy(slot[idx+1:], slot[idx:])
	slot[idx].offset = offset
	slot[idx].hash16 = hash16
	slot[idx].keyLen = keyLen
	slot[idx].block = seg.curBlock
	// _ = seg.getSlot(slotId)
}

func (seg *segment) delEntryPtrByOffset(slotId uint8, hash16 uint16, offset int64) {
	slot := seg.getSlot(slotId)
	idx, match := seg.lookupByOff(slot, hash16, offset)
	if !match {
		return
	}
	seg.delEntryPtr(slotId, slot, idx)
}

func (seg *segment) delEntryPtr(slotId uint8, slot []entryPtr, idx int) {
	ptr := &slot[idx]
	hdr := seg.getHdr(ptr)
	hdr.deleted = true
	copy(slot[idx:], slot[idx+1:])
	seg.slotsLen[slotId]--
	atomic.AddInt64(&seg.entryCount, -1)
}

func (seg *segment) getSlot(slotId uint8) []entryPtr {
	slotOff := int32(slotId) * seg.slotCap
	return seg.slotsData[slotOff : slotOff+seg.slotsLen[slotId] : slotOff+seg.slotCap]
}

func isExpired(keyExpireAt, now uint32) bool {
	return keyExpireAt != 0 && keyExpireAt <= now
}

func (seg *segment) resetStatistics() {
	atomic.StoreInt64(&seg.evictionNum, 0)
	atomic.StoreInt64(&seg.evictionCount, 0)
	atomic.StoreInt64(&seg.evictionWaitCount, 0)
	atomic.StoreInt64(&seg.expireCount, 0)
	atomic.StoreInt64(&seg.missCount, 0)
	atomic.StoreInt64(&seg.hitCount, 0)
	atomic.StoreInt64(&seg.writeCount, 0)
	atomic.StoreInt64(&seg.writeErrCount, 0)
	atomic.StoreInt64(&seg.overwriteCount, 0)
	atomic.StoreInt64(&seg.skipWriteCount, 0)
}
