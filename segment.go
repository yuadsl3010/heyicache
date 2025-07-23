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
var maxLocateRetry = 3
var sleepLocate = 1 * time.Millisecond // ms

// it's quite different from freecache, cause we don't need to use ring buffer
// once found a segment is full, we will allocate a new segment and release the old one
type segment struct {
	bufs              [versionCount]*buffer
	segId             int32
	version           int32 // increase when segment has been evictioned, but only 0, 1, or 2
	missCount         int64
	hitCount          int64
	entryCount        int64
	totalCount        int64            // number of entries in ring buffer, including deleted entries.
	totalTime         int64            // used to calculate least recent used entry.
	timer             Timer            // Timer giving current time
	totalEviction     int64            // used for debug
	totalEvictionWait int64            // used for debug
	totalExpired      int64            // used for debug
	overwrites        int64            // used for debug
	slotsLen          [slotCount]int32 // the length for every slot
	slotCap           int32            // max number of entry pointers a slot can hold.
	slotsData         []entryPtr
	idleBuf           *int32
}

func newSegment(bufSize, segId int32, idleBuf *int32, shuffleRatio float32, timer Timer) segment {
	seg := segment{
		bufs:      [versionCount]*buffer{},
		segId:     segId,
		timer:     timer,
		slotCap:   1,
		slotsData: make([]entryPtr, slotCount),
		idleBuf:   idleBuf,
	}

	for i := 0; i < int(versionCount); i++ {
		seg.bufs[i] = NewBuffer(bufSize, i == int(seg.version), shuffleRatio)
	}

	return seg
}

func (seg *segment) getBuffer() *buffer {
	return seg.bufs[seg.version]
}

func (seg *segment) enough(allSize int32) bool {
	return allSize+seg.getBuffer().index < seg.getBuffer().size
}

//go:inline
func (seg *segment) processUsed(version int32, k int64) {
	seg.bufs[version].used += k
	if seg.version == version {
		return
	}
	if seg.bufs[version].used == 0 {
		seg.bufs[version].Clear()
		for {
			// make sure success
			idle := atomic.LoadInt32(seg.idleBuf)
			ok := atomic.CompareAndSwapInt32(seg.idleBuf, idle, idle+1)
			if ok {
				break
			}
		}
	}
}

func (seg *segment) getNextVersion() int32 {
	return (seg.version + 1) % versionCount
}

func (seg *segment) eviction() error {
	version := seg.getNextVersion()
	if seg.bufs[version].used > 0 {
		// it's only two cases
		// 1. the speed of generating is too fast: expand the cache size
		// 2. some interfaces getted from Get() but not released by Done(): check the code logic
		// for case 1, I think 3 buffers are enough, just expand the cache size will decrease the write error ratio
		// fmt.Println("eviction wait, segId", seg.segId, "version", seg.version, "used", seg.used, "tmp1Used", seg.tmp1Used, "tmp2Used", seg.tmp2Used)
		atomic.AddInt64(&seg.totalEvictionWait, 1)
		return ErrSegmentFull
	}

	idle := atomic.LoadInt32(seg.idleBuf)
	if idle <= 0 {
		// no space to allocate, return error
		// better to expand the MaxSizeBeyondRatio
		atomic.AddInt64(&seg.totalEvictionWait, 1)
		return ErrSegmentBusy
	}

	ok := atomic.CompareAndSwapInt32(seg.idleBuf, idle, idle-1)
	if !ok {
		// someone move faster than us
		atomic.AddInt64(&seg.totalEvictionWait, 1)
		return ErrSegmentUnlucky
	}

	// fmt.Println("eviction done, segId", seg.segId, "version", seg.version, "used", seg.used, "tmp1Used", seg.tmp1Used, "tmp2Used", seg.tmp2Used)
	seg.bufs[version].ReAlloc()
	seg.slotsData = make([]entryPtr, len(seg.slotsData))
	seg.slotsLen = [slotCount]int32{} // reset slots length
	atomic.AddInt64(&seg.totalEviction, seg.entryCount)
	seg.entryCount = 0
	seg.version = version
	return nil
}

func (seg *segment) alloc(key []byte, valueSize int32) ([]byte, int32, error) {
	// param check
	if len(key) > 65535 {
		return nil, 0, ErrLargeKey
	}

	// check buffer size
	allSize := int32(ENTRY_HDR_SIZE+len(key)) + valueSize
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
	index := seg.getBuffer().index
	return seg.getBuffer().Alloc(allSize), index, nil
}

func (seg *segment) insert(bs []byte, index int32, key []byte, valueSize int32, hashVal uint64, expireSeconds int) {
	// check if the key already exists
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if match {
		// the exist memory can not be modified, so we need to delete it
		atomic.AddInt64(&seg.overwrites, 1)
		seg.delEntryPtr(slotId, slot, idx)
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

	// insert the node
	atomic.AddInt64(&seg.totalTime, int64(now))
	atomic.AddInt64(&seg.totalCount, 1)
	seg.insertEntryPtr(slotId, hash16, index, idx, hdr.keyLen)
}

func (seg *segment) write(bs []byte, key []byte, value interface{}, fnSet FuncSet) {
	// cache 1. write key
	copy(bs[ENTRY_HDR_SIZE:], key)

	// cache 2. write value
	fnSet(value, bs[ENTRY_HDR_SIZE+len(key):], true)
}

func (seg *segment) get(key []byte, fnGet FuncGet, hashVal uint64, peek bool) (interface{}, error) {
	hdr, ptrOffset, err := seg.locate(key, hashVal, peek)
	if err != nil {
		return nil, err
	}

	start := ptrOffset + int32(ENTRY_HDR_SIZE+hdr.keyLen)
	bs := seg.getBuffer().Slice(start, int32(hdr.valLen))
	if !peek {
		atomic.AddInt64(&seg.hitCount, 1)
	}

	return fnGet(bs), nil
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

func (seg *segment) locate(key []byte, hashVal uint64, peek bool) (entryHdr, int32, error) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		if !peek {
			atomic.AddInt64(&seg.missCount, 1)
		}
		return entryHdr{}, 0, ErrNotFound
	}

	ptr := &slot[idx]
	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.getBuffer().ReadAt(hdrBuf[:], ptr.offset)
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	if !peek {
		now := seg.timer.Now()
		if isExpired(hdr.expireAt, now) {
			seg.delEntryPtr(slotId, slot, idx)
			atomic.AddInt64(&seg.totalExpired, 1)
			atomic.AddInt64(&seg.missCount, 1)
			return entryHdr{}, 0, ErrNotFound
		}
		atomic.AddInt64(&seg.totalTime, int64(now-hdr.accessTime))
		hdr.accessTime = now
		seg.getBuffer().WriteAt(hdrBuf[:], ptr.offset)
	}
	return *hdr, ptr.offset, nil
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
		match = int(ptr.keyLen) == len(key) && seg.getBuffer().EqualAt(key, ptr.offset+ENTRY_HDR_SIZE)
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

func (seg *segment) insertEntryPtr(slotId uint8, hash16 uint16, offset int32, idx int, keyLen uint16) {
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
	_ = seg.getSlot(slotId)
}

func (seg *segment) delEntryPtr(slotId uint8, slot []entryPtr, idx int) {
	offset := slot[idx].offset
	var entryHdrBuf [ENTRY_HDR_SIZE]byte
	seg.getBuffer().ReadAt(entryHdrBuf[:], offset)
	entryHdr := (*entryHdr)(unsafe.Pointer(&entryHdrBuf[0]))
	entryHdr.deleted = true
	seg.getBuffer().WriteAt(entryHdrBuf[:], offset)
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
	atomic.StoreInt64(&seg.totalEviction, 0)
	atomic.StoreInt64(&seg.totalEvictionWait, 0)
	atomic.StoreInt64(&seg.totalExpired, 0)
	atomic.StoreInt64(&seg.overwrites, 0)
	atomic.StoreInt64(&seg.hitCount, 0)
	atomic.StoreInt64(&seg.missCount, 0)
}
