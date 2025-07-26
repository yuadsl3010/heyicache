# HeyiCache - A zero GC overhead, no encoding/decoding, high-performance in-memory cache component designed for Golang.
If you're like me, needing an in-memory cache in Golang to store millions or even tens of millions of items, and you want to avoid both:

Excessive pointers slowing down GC, and

Forced encoding/decoding conversions required by typical zero-GC caches on every read/write,

Then HeyiCache is all you need!

## Why HeyiCache?
HeyiCache draws inspiration from FreeCache's cache structure design, inheriting many of its advantages:
1. Zero GC overhead
2. Concurrency-safe access (goroutine-safe)
3. Expiration support
4. Optimized Get/Set Value Objects: Replaces []byte with struct pointers. By mapping the struct pointer's contents to pre-allocated []byte memory, it eliminates the performance penalty of encoding/decoding during Get/Set operations.

## Performance
Under single-threaded conditions, HeyiCache is slightly slower than a native map or GoCache.

However, as the number of threads increases, HeyiCache's multi-shard architecture significantly boosts cache throughput.

Furthermore, by avoiding encoding/decoding overhead, HeyiCache exhibits significantly lower latency and far fewer memory allocations compared to FreeCache and BigCache.

Testing used a struct containing nested Protobuf messages – complex but representative of real-world scenarios.

See the Performance Comparison Report:
https://github.com/yuadsl3010/heyicache-benchmark

Test Environment:

    goos: darwin
    goarch: arm64
    pkg: github.com/yuadsl3010/heyicache-benchmark
    cpu: Apple M1 Pro

### 100w item, 1 goroutine: 1 write, 99 read, after 99th read do a cache result check - 10s

    BenchmarkMap-10                   712758             22535 ns/op           10332 B/op        435 allocs/op
    BenchmarkGoCache-10               525926             25199 ns/op           10437 B/op        435 allocs/op
    BenchmarkFreeCache-10              66950            188858 ns/op          362027 B/op       6182 allocs/op
    BenchmarkBigCache-10               56229            220568 ns/op          367655 B/op       6281 allocs/op
    BenchmarkHeyiCache-10             487784             26343 ns/op           12563 B/op        443 allocs/op

    Read: success=48290616 miss=0 missRate=0.00%
    Write: success=487784 fail=0 failRate=0.00%
    Check: success=487784 fail=0 failRate=0.00%

### 100w item, 10 goroutine: 1 write, 99 read, after 99th read do a cache result check - 10s

    BenchmarkMap-10                    71468            143057 ns/op          102474 B/op       4353 allocs/op
    BenchmarkGoCache-10                59056            199459 ns/op          101844 B/op       4352 allocs/op
    BenchmarkFreeCache-10              28719            450582 ns/op         3586414 B/op      61814 allocs/op
    BenchmarkBigCache-10               30032            385628 ns/op         3611240 B/op      62805 allocs/op
    BenchmarkHeyiCache-10             155607             78537 ns/op          123514 B/op       4437 allocs/op

    Read: success=52253444 miss=0 missRate=0.00%
    Write: success=1532655 fail=0 failRate=0.00%
    Check: success=1543626 fail=0 failRate=0.00%

### 100w item, 100 goroutine: 1 write, 99 read, after 99th read do a cache result check - 10s

    BenchmarkMap-10                     6025           2195842 ns/op         1012247 B/op      42823 allocs/op
    BenchmarkGoCache-10                 4082           3160241 ns/op          999648 B/op      42456 allocs/op
    BenchmarkFreeCache-10               2739           4742585 ns/op        35077612 B/op     616594 allocs/op
    BenchmarkBigCache-10                2624           5127104 ns/op        35326953 B/op     626420 allocs/op
    BBenchmarkHeyiCache-10             15436            799174 ns/op         1219251 B/op      44084 allocs/op

    Read: success=59521064 miss=80582 missRate=0.14% // now we get some cache miss cause the eviction strategy
    Write: success=1516698 fail=406 failRate=0.03%
    Check: success=1528075 fail=0 failRate=0.00%

## Example Usage
### 1. Prepare your value struct
Assume the value is TestCacheStruct
```go
type TestCacheStruct struct {
	id   int
	name string
}
```

### 2. Generate memory mapping functions for your struct
It's recommended to create a file (e.g., heyicache_fn_test.go) with this content:

go generate ./... (Command to run code generation)
```go
package main

import (
	"testing"

	"github.com/yuadsl3010/heyicache"
)

func TestFnGenerateTool(t *testing.T) {
	heyicache.GenCacheFn(TestCacheStruct{}, true)
}
```
This will generate a Go file containing the three required functions: HeyiCacheFnGetTestCacheStruct, HeyiCacheFnSizeTestCacheStruct, and HeyiCacheFnSetTestCacheStruct

### 3. Use the cache for reads/writes
```go
package main

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/yuadsl3010/heyicache"
)

func main() {
	cache, err := heyicache.NewCache(
		heyicache.Config{
			Name:    "heyi_cache_test", // it should be unique
			MaxSize: int32(100),        // 100MB cache, the min size is 32MB
		},
	)
	if err != nil {
		panic(err)
	}

	key := "test_key"
	value := &TestCacheStruct{
		Id:   1,
		Name: "foo string",
	}

	// set a value
	err = cache.Set([]byte(key), value, HeyiCacheFnTestCacheStructIfc_, 60) // 60 seconds expiration
	if err != nil {
		fmt.Println("Error setting value:", err)
		return
	}

	// get a vlue
	ctx := heyicache.NewLeaseCtx(context.Background()) // init a new context with heyi cache lease
	leaseCtx := heyicache.GetLeaseCtx(ctx)
	leaseCache := leaseCtx.GetLease(cache)
	data, err := cache.Get(leaseCache, []byte(key), HeyiCacheFnTestCacheStructIfc_)
	if err != nil {
		fmt.Println("Error getting value:", err)
		return
	}

	testStruct, ok := data.(*TestCacheStruct)
	if !ok {
		fmt.Println("Error asserting cache value")
		return
	}

	fmt.Println("Got value from cache:", testStruct)
	heyicache.GetLeaseCtx(ctx).Done()
}
```

## Memory Mapping Implementation
HeyiCache first allocates a []byte slice of the required length from its buffer.

It then maps the memory space of the struct directly onto this pre-allocated []byte segment.

After a Set operation, Get becomes simple: retrieve the []byte slice and cast the first StructSize bytes directly to a struct pointer.

The memory mapping principle is illustrated below:
![image](https://github.com/yuadsl3010/heyicache-benchmark/blob/master/img/memory-mapping.svg)

## Cache Read/Write Implementation
Heyicache initializes 256 segments. Each segment initializes:

1. 10 buffers
2. 1 entry array
3. 1 slotLen map (length 256)

### Entry Array and slotLen Overview
(Fully reuses freecache's logic and implementation)

1. The entry array length is always a multiple of 256 (number of slots) × 2.
2. Example: With 1024-entry length:
    1. Entries [0-3] belong to slot 0
    2. Entries [4-7] belong to slot 1
3. When locking slot 1 with slotLen[1] = 3, only entries [4-6] require binary search.

The cache read/write principle is illustrated below:
![image](https://github.com/yuadsl3010/heyicache-benchmark/blob/master/img/get:set.svg)

## Data Eviction Implementation
### Buffer Management (10 buffers per segment)
1. Each buffer has equal size; their combined capacity equals one segment (1/256 of total cache size).
2. curBlock cycles through 0-9 (buffer indices), incrementing when a buffer fills.
3. nextBlock is (curBlock + 1) % 10.

### Eviction Trigger
When curBlock exceeds the EvictionTriggerTiming threshold (default: 50%):
1. Eviction starts for nextBlock
2. nextBlock becomes read-prohibited
3. After confirming zero access dependencies, nextBlock's buffer and entries are reclaimed.

### Example (Threshold=80%):
1. At 98% cache utilization:
    1. Eviction initiates
    2. nextBlock reclaimed when access-free
2. Cache utilization drops to 88%

The data eviction principle is illustrated below:
![image](https://github.com/yuadsl3010/heyicache-benchmark/blob/master/img/eviction.svg)

## Lease Implementation
### Tracking Block Access Dependencies
1. Both cache and leases maintain a [segment count][block count]int32 array.
2. Example:

When fetching an object from segment 3, block 5:

[3][5] increments in both cache and lease.

### Context lifecycle end:

Cache decrements lease counters

If nextBlock is under eviction and reaches used=0:

    Immediate reclamation occurs.

The lease principle is illustrated below:
![image](https://github.com/yuadsl3010/heyicache-benchmark/blob/master/img/lease.svg)

## Limitations
Such significant performance! but at what cost?

### 1. Value struct type restrictions
Value must be a *struct (pointer to a struct).

Map fields within the struct cannot be cached and will be forcibly set to nil.

1. Why *struct? Simplifies automatic generation of memory mapping functions (Step 2 in the integration example).
2. Why no maps? Golang map memory is non-contiguous and highly fragmented, making it impossible to store using a contiguous memory block. Recommended alternatives include using slices/arrays. (Better memory management approaches are welcome for discussion!).

### 2. Values are Read-Only
After memory mapping, all pointers within the struct point to the pre-allocated contiguous memory block. Modifying even a string field could cause subsequent Get operations to access garbage-collected memory, leading to a panic. 

Therefore, values must be treated as read-only.

Tip: In practice, modifying primitive types directly embedded in the struct's memory (like uint64, bool) is possible if you understand the risks, as they reside within the contiguous block and aren't subject to GC in the problematic way. However, users must be absolutely certain of what they are modifying to avoid panics.

### 3. Slightly Higher Eviction Probability
Due to memory mapping, the smallest unit of eviction in heyicache is a buffer within a segment (like freecache, there are 256 segments, each containing 10 buffers. For example, if the total cache space is 256MB, then each segment is 1MB, and a single buffer – the memory evicted at once – is 100KB. In contrast, freecache evicts one using an approximate FIFO algorithm).

Because it's impossible to know which data in the segment is being accessed, when a buffer fills up, a new buffer must be created. The old buffer is only recycled after it's confirmed no longer accessible.

This characteristic leads to:

Higher probability of data expiration when memory is full: Compared to freecache or bigcache, the likelihood of data expiring is slightly higher.

Based on my own practical experience with business applications:

Negligible impact of slightly lower cache hit rate: The performance improvements far outweigh the negligible loss caused by the slightly lower cache hit rate.

### 4. Mandatory Lease Return
You must actively return the lease (lease.Done()) once you are done using the data retrieved via GetLease.

For gRPC services, it's highly recommended to add a middleware that calls heyicache.GetLeaseCtx(ctx).Done() after the response has been marshaled (see Integration Example Step 3).

Failure to return leases prevents HeyiCache from knowing when buffers are safe to recycle, potentially blocking new allocations when the buffer fills

## Recommendations
Most use cases can integrate quickly using the provided example.

Highly Recommended: Implement regular monitoring/reporting of HeyiCache metrics (memory usage, evictions, errors). This helps determine if memory needs adjustment or if data access patterns should be optimized.

## Questions or Suggestions?
We welcome discussion and collaboration! Feel free to reach out: yuadsl3010@gmail.com
