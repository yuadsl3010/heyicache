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
Under single-threaded conditions, HeyiCache is slightly slower than a native map or GoCache. However, as the number of threads increases, HeyiCache's multi-shard architecture significantly boosts cache throughput. Furthermore, by avoiding encoding/decoding overhead, HeyiCache exhibits significantly lower latency and far fewer memory allocations compared to FreeCache and BigCache.

Testing used a struct containing nested Protobuf messages – complex but representative of real-world scenarios.

See the Performance Comparison Report:
https://github.com/yuadsl3010/heyicache-benchmark

Test Environment:

    goos: darwin
    goarch: arm64
    pkg: github.com/yuadsl3010/heyicache-benchmark
    cpu: Apple M1 Pro

### 100w item, 1 goroutine: 1 write, 99 read, after 99th read do a cache result check

    BenchmarkMap-10                    72154             16882 ns/op           10937 B/op        435 allocs/op
    BenchmarkGoCache-10                53970             22752 ns/op           10361 B/op        435 allocs/op
    BenchmarkFreeCache-10               5823            183588 ns/op          368174 B/op       6175 allocs/op
    BenchmarkBigCache-10                5756            214602 ns/op          410137 B/op       6276 allocs/op
    BenchmarkHeyiCache-10              46688             26497 ns/op           19363 B/op        443 allocs/op

### 100w item, 10 goroutine: 1 write, 99 read, after 99th read do a cache result check

    BenchmarkMap-10                     7735            160470 ns/op          107603 B/op       4299 allocs/op
    BenchmarkGoCache-10                 5965            220789 ns/op          100109 B/op       4281 allocs/op
    BenchmarkFreeCache-10               3002            473894 ns/op         3524773 B/op      61674 allocs/op
    BenchmarkBigCache-10                2677            424352 ns/op         3629275 B/op      62649 allocs/op
    BenchmarkHeyiCache-10              10000            100087 ns/op          190508 B/op       4393 allocs/op

### 100w item, 100 goroutine: 1 write, 99 read, after 99th read do a cache result check

    BenchmarkMap-10                      590           2183559 ns/op         1030850 B/op      35661 allocs/op
    BenchmarkGoCache-10                  418           3314638 ns/op          907144 B/op      32406 allocs/op
    BenchmarkFreeCache-10                260           4523621 ns/op        34550073 B/op     600289 allocs/op
    BenchmarkBigCache-10                 205           4915564 ns/op        35840800 B/op     609953 allocs/op
    BenchmarkHeyiCache-10               1360            796445 ns/op         1856661 B/op      40954 allocs/op

    Read: success=5243161 miss=2227 missRate=0.04% // now we get some cache miss cause the eviction strategy
    Write: success=145197 fail=0 failRate=0.00%
    Check: success=146236 fail=0 failRate=0.00%

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
	err = cache.Set([]byte(key), value, HeyiCacheFnSetTestCacheStruct, HeyiCacheFnSizeTestCacheStruct, 60) // 60 seconds expiration
	if err != nil {
		fmt.Println("Error setting value:", err)
		return
	}

	// get a vlue
	ctx := heyicache.NewLeaseCtx(context.Background()) // init a new context with heyi cache lease
	leaseCtx := heyicache.GetLeaseCtx(ctx)
	leaseCache := leaseCtx.GetLease(cache)
	data, err := cache.Get(leaseCache, []byte(key), HeyiCacheFnGetTestCacheStruct)
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
HeyiCache first allocates a []byte slice of the required length from its buffer. It then maps the memory space of the struct directly onto this pre-allocated []byte segment. After a Set operation, Get becomes simple: retrieve the []byte slice and cast the first StructSize bytes directly to a struct pointer.

The memory mapping principle is illustrated below:
![image](https://github.com/yuadsl3010/heyicache-benchmark/blob/master/img/heyicache.svg)


## Limitations
Such significant performance! but at what cost?

### 1. Value struct type restrictions
Value must be a *struct (pointer to a struct).
Map fields within the struct cannot be cached and will be forcibly set to nil.

1. Why *struct? Simplifies automatic generation of memory mapping functions (Step 2 in the integration example).
2. Why no maps? Golang map memory is non-contiguous and highly fragmented, making it impossible to store using a contiguous memory block. Recommended alternatives include using slices/arrays. (Better memory management approaches are welcome for discussion!).

### 2. Values are Read-Only
After memory mapping, all pointers within the struct point to the pre-allocated contiguous memory block. Modifying even a string field could cause subsequent Get operations to access garbage-collected memory, leading to a panic. Therefore, values must be treated as read-only.

Tip: In practice, modifying primitive types directly embedded in the struct's memory (like uint64, bool) is possible if you understand the risks, as they reside within the contiguous block and aren't subject to GC in the problematic way. However, users must be absolutely certain of what they are modifying to avoid panics.

### 3. Rare Write Errors, Moderate Memory Overhead, Slightly Higher Eviction Probability
Due to its memory mapping design, HeyiCache's eviction unit is a whole segment (like FreeCache, it has 256 segments; e.g., a 256MB cache evicts 1MB at a time). Since it cannot track which items within a segment are actively referenced, when the buffer fills, it must allocate a new buffer. The old buffer is only recycled once confirmed inaccessible.

This leads to:
1. Rare Write Failures: If the old buffer hasn't been recycled yet and a new buffer cannot be created (default allows up to 3 buffers, meaning memory could temporarily balloon to 3x the configured size under extreme load; normal operation usually stays within limits).
2. Slight Cache Miss Increase: Evicting an entire segment inevitably removes some valid data prematurely, slightly reducing the cache hit rate.

Practical Note: In real-world usage, the significant performance gains often outweigh the minor reduction in cache hit rate.

### 4. Mandatory Lease Return
You must actively return the lease (lease.Done()) once you are done using the data retrieved via GetLease.

For gRPC services, it's highly recommended to add a middleware that calls heyicache.GetLeaseCtx(ctx).Done() after the response has been marshaled (see Integration Example Step 3). Failure to return leases prevents HeyiCache from knowing when buffers are safe to recycle, potentially blocking new allocations when the buffer fills

## Recommendations
Most use cases can integrate quickly using the provided example.

Highly Recommended: Implement regular monitoring/reporting of HeyiCache metrics (memory usage, evictions, errors). This helps determine if memory needs adjustment or if data access patterns should be optimized.

## Questions or Suggestions?
We welcome discussion and collaboration! Feel free to reach out
