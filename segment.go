package heyicache

import (
	"errors"
	"time"
	"unsafe"
)

var ErrSegmentFull = errors.New("segment is full, please wait for automatic eviction")
var ErrSegmentBusy = errors.New("segment is busy, please check if the beyond ratio is set too small")
var ErrSegmentUnlucky = errors.New("segment is unlucky, please retry")
var ErrValueTooBig = errors.New("value is too big, please use smaller value or increase cache size")
var ErrSegmentCleaning = errors.New("segment has been expanded, re-allocate space, please retry")
var ErrDuplicateWrite = errors.New("write interval less than MinWriteSecondsForSameKey, no need to write again")
var ErrStorageDupWrite = errors.New("storage mode does not allow duplicate write")
var maxLocateRetry = 3
var sleepLocate = 1 * time.Millisecond // ms

// it's quite different from freecache, cause we don't need to use ring buffer
// once found a segment is full, we will allocate a new segment and release the old one
type segment struct {
	bufs              []*buffer
	segId             int32
	curBlock          int32
	nextBlock         int32
	isEviction        bool
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
	slotCap           int32 // max number of entry pointers a slot can hold.
	evictionSize      int64
	slotsLen          [slotCount]int32 // the length for every slot
	slotOffsets       [slotCount]int32 // 预计算的slot偏移量
	slotsData         []entryPtr
}

func newSegment(bufSize int64, segId int32, evictionTriggerTiming float32, minWriteInterval int32, timer Timer) segment {
	everyBufSize := bufSize / int64(blockCount)
	seg := segment{
		bufs:             []*buffer{},
		segId:            segId,
		timer:            timer,
		slotCap:          1,
		slotsData:        make([]entryPtr, slotCount),
		minWriteInterval: minWriteInterval,
		curBlock:         0,
		nextBlock:        1,
		evictionSize:     int64(float64(everyBufSize) * float64(evictionTriggerTiming)),
	}

	for i := 0; i < int(blockCount); i++ {
		seg.bufs = append(seg.bufs, NewBuffer(everyBufSize))
	}

	seg.updateSlotOffsets()
	return seg
}

func (seg *segment) readyForClose() bool {
	for _, buf := range seg.bufs {
		if buf.used > 0 {
			return false
		}
	}

	return true
}

func (seg *segment) close() {
	for _, buf := range seg.bufs {
		buf.data = nil // release the memory
	}
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
	return allSize+seg.getCurBuffer().index <= seg.getCurBuffer().size
}

func (seg *segment) isInEviction(block int32) bool {
	return seg.nextBlock == block && seg.isEviction
}

// only cache mode can use this function
func (seg *segment) eviction() error {
	buf := seg.bufs[seg.nextBlock]
	if buf.used > 0 {
		// it's only two cases
		// 1. the speed of generating is too fast: expand the cache size
		// 2. some interfaces getted from Get() but not released by Done(): check the code logic
		// for case 1, I think 3 buffers are enough, just expand the cache size will decrease the write error ratio
		seg.evictionWaitCount += 1
		return ErrSegmentFull
	}

	// clean the next block
	offset := int64(0)
	for offset+ENTRY_HDR_SIZE <= buf.index {
		hdr := (*entryHdr)(unsafe.Pointer(&buf.data[offset]))
		if !hdr.deleted {
			seg.delEntryPtrByOffset(hdr.slotId, hdr.hash16, offset)
			seg.evictionNum += 1
		}
		offset = offset + ENTRY_HDR_SIZE + int64(hdr.keyLen) + int64(hdr.valLen)
	}

	buf.index = 0
	seg.evictionCount += 1
	seg.isEviction = false
	seg.curBlock = seg.nextBlock
	seg.nextBlock = (seg.curBlock + 1) % blockCount
	return nil
}

// only storage mode can use this function
func (seg *segment) expandMemory(isStorageUnlimited bool) error {
	if seg.curBlock == int32(len(seg.bufs)-1) {
		if !isStorageUnlimited {
			return ErrSegmentFull
		}

		// if storage mode can use unlimited size, just automatically expands the buffers
		seg.bufs = append(seg.bufs, NewBuffer(seg.getCurBuffer().size))
	}

	seg.curBlock += 1
	return nil
}

func (seg *segment) set(key []byte, value interface{}, hashVal uint64, expireSeconds int, isStorage, isStorageUnlimited bool, fn HeyiCacheFnIfc) error {
	// check large key
	if len(key) > 65535 {
		return ErrLargeKey
	}

	valueSize := fn.Size(value, true)
	// check large key + value
	maxKeyValLen := int(seg.getCurBuffer().size - ENTRY_HDR_SIZE)
	if len(key)+int(valueSize) > maxKeyValLen {
		// Do not accept large entry.
		return ErrLargeEntry
	}

	// check if the key already exists
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if match {
		// storage mode, we do not allow duplicate write
		if isStorage {
			return ErrStorageDupWrite
		}

		skip := false
		if seg.minWriteInterval > 0 {
			ptr := &slot[idx]
			hdr := seg.getHdr(ptr)
			if seg.timer.Now()-hdr.createTime <= uint32(seg.minWriteInterval) {
				// the write interval is too short, skip this write
				skip = true
			}
		}

		if !skip {
			// the exist memory can not be modified, so we need to delete it
			seg.overwriteCount += 1
			seg.delEntryPtr(slotId, slot, idx)
		} else {
			// the write interval is too short, skip this write
			seg.skipWriteCount += 1
			return ErrDuplicateWrite
		}
	}

	// check buffer size
	entryLen := ENTRY_HDR_SIZE + int64(len(key)) + int64(valueSize)
	if !seg.enough(entryLen) {
		if !isStorage {
			// trigger eviction if no enough space
			seg.isEviction = true
			// not enough space in segment, return error.
			// the caller should try to allocate a new segment.
			err := seg.eviction()
			if err != nil {
				return err
			}

			if !seg.enough(entryLen) {
				// still not enough space, return error.
				return ErrValueTooBig
			}

			// every time we expand the slot, we need to re-check the key
			slot = seg.getSlot(slotId)
			idx, _ = seg.lookup(slot, hash16, key)
		} else {
			err := seg.expandMemory(isStorageUnlimited)
			if err != nil {
				return err
			}
			// no need to re-check the buffer, because a new buffer must have enough space, if not, it will return ErrLargeEntry before
		}
	}

	// prepare expire
	now := seg.timer.Now()
	expireAt := uint32(0)
	if expireSeconds > 0 && !isStorage {
		expireAt = now + uint32(expireSeconds)
	}
	// write to cache
	buf := seg.getCurBuffer()
	offset := buf.index
	bs := buf.Alloc(entryLen)
	// if the cache size higher than the eviction size, set the isEviction flag
	if buf.index > seg.evictionSize && !isStorage {
		seg.isEviction = true
	}
	// 1. write entry header
	hdr := (*entryHdr)(unsafe.Pointer(&bs[0]))
	hdr.deleted = false
	hdr.slotId = slotId
	hdr.hash16 = hash16
	hdr.keyLen = uint16(len(key))
	hdr.createTime = now
	hdr.expireAt = expireAt
	hdr.valLen = uint32(valueSize)

	// 2. write key
	copy(bs[ENTRY_HDR_SIZE:], key)

	// 3. write value
	fn.Set(value, bs[ENTRY_HDR_SIZE+int64(len(key)):], true)

	// insert the node
	seg.insertEntryPtr(slotId, hash16, offset, idx, hdr.keyLen)
	seg.writeCount += 1

	return nil
}

func (seg *segment) get(key []byte, fn HeyiCacheFnIfc, hashVal uint64) (interface{}, error) {
	hdr, ptr, err := seg.locate(key, hashVal)
	if err != nil {
		seg.missCount += 1
		return nil, err
	}

	seg.hitCount += 1
	start := ptr.offset + ENTRY_HDR_SIZE + int64(hdr.keyLen)
	bs := seg.getBuffer(ptr).Slice(start, int64(hdr.valLen))
	return fn.Get(bs), nil
}

func (seg *segment) del(key []byte, hashVal uint64) bool {
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

func (seg *segment) locate(key []byte, hashVal uint64) (*entryHdr, *entryPtr, error) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		return nil, nil, ErrNotFound
	}

	ptr := &slot[idx]
	if seg.isInEviction(ptr.block) {
		// data is in next block and current block is in eviction
		return nil, nil, ErrNotFound
	}

	hdr := seg.getHdr(ptr)
	if hdr.deleted {
		return nil, nil, ErrNotFound
	}

	now := seg.timer.Now()
	if hdr.expireAt != 0 && hdr.expireAt <= now {
		seg.delEntryPtr(slotId, slot, idx)
		seg.expireCount += 1
		return nil, nil, ErrNotFound
	}

	return hdr, ptr, nil
}

func entryPtrIdx(slot []entryPtr, hash16 uint16) int {
	idx := 0
	high := len(slot)
	for idx < high {
		mid := (idx + high) >> 1
		if slot[mid].hash16 < hash16 {
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
	keyLen := uint16(len(key))
	slotLen := len(slot)
	for idx < slotLen {
		ptr := &slot[idx]
		if ptr.hash16 != hash16 {
			break
		}
		match = (ptr.keyLen == keyLen) && seg.getBuffer(ptr).EqualAt(key, ptr.offset+ENTRY_HDR_SIZE)
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

func (seg *segment) updateSlotOffsets() {
	// 更新预计算的偏移量
	for i := 0; i < int(slotCount); i++ {
		seg.slotOffsets[i] = int32(i) * seg.slotCap
	}
}

func (seg *segment) expandSlot() {
	newSlotData := make([]entryPtr, seg.slotCap*2*slotCount)
	for i := 0; i < int(slotCount); i++ {
		off := int32(i) * seg.slotCap
		copy(newSlotData[off*2:], seg.slotsData[off:off+seg.slotsLen[i]])
	}
	seg.slotCap *= 2
	seg.slotsData = newSlotData
	seg.updateSlotOffsets()
}

func (seg *segment) insertEntryPtr(slotId uint8, hash16 uint16, offset int64, idx int, keyLen uint16) {
	if seg.slotsLen[slotId] == seg.slotCap {
		seg.expandSlot()
	}
	seg.slotsLen[slotId]++
	seg.entryCount += 1
	slot := seg.getSlot(slotId)
	copy(slot[idx+1:], slot[idx:])
	slot[idx].offset = offset
	slot[idx].hash16 = hash16
	slot[idx].keyLen = keyLen
	slot[idx].block = seg.curBlock
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
	seg.entryCount -= 1
}

func (seg *segment) getSlot(slotId uint8) []entryPtr {
	slotOff := seg.slotOffsets[slotId]
	return seg.slotsData[slotOff : slotOff+seg.slotsLen[slotId] : slotOff+seg.slotCap]
}

func (seg *segment) resetStatistics() {
	seg.evictionNum = 0
	seg.evictionCount = 0
	seg.evictionWaitCount = 0
	seg.expireCount = 0
	seg.missCount = 0
	seg.hitCount = 0
	seg.writeCount = 0
	seg.writeErrCount = 0
	seg.overwriteCount = 0
	seg.skipWriteCount = 0
}
