# HeyiCache - 一款为golang设计，零GC、无编解码、高性能的内存cache组件
如果你和我一样，需要在golang中使用内存cache来存储百万甚至千万级别的item
既不想cache过多的指针导致gc过慢，也不想切换zero gc cache迫使读写时强制做编解码转换
那么，heyicache is all your need

## 为什么是HeyiCache？
heyicache参考自freecache的缓存结构设计，继承了freecache的许多优点
1. zero gc overhead
2. 协程安全的并发访问
3. 过期功能支持
同时，将Get、Set的value对象从[]byte优化为struct指针，通过将struct指针内容指向提前申请好的[]byte内存，规避Get、Set前后编解码带来的性能损耗

## 性能
单线程下，heyicache会略慢于map和gocache，但随着线程慢慢增多，heyicache的多分片优势会大幅度提升cache吞吐量
而由于避免了编解码带来的性能损失，heyicache的无论是时延还是内存申请都远远小于freecache和bigcache
测试的结构体是struct内部嵌套一些protobuf，相对复杂但比较贴合业务场景
性能对比报告详见：https://github.com/yuadsl3010/heyicache-benchmark

测试环境

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

## 接入例子
### 1. 准备好value结构体
假设value是TestCacheStruct
```go
type TestCacheStruct struct {
	id   int
	name string
}
```
### 2. 为value结构体生成内存映射函数
推荐创建一个heyicache_fn_test.go文件，内容如下
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
执行后将得到一个go文件，里面包含HeyiCacheFnGetTestCacheStruct、HeyiCacheFnSizeTestCacheStruct和HeyiCacheFnSetTestCacheStruct三个函数

### 3. 使用cache进行读写
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
## 内存映射实现原理
heyicache先从buffer中申请好指定长度[]byte，再将struct的空间内存，映射到这一段[]byte中
完成Set之后，Get就相对简单了，只需要将[]byte内存取出来，然后取前StructSize的[]byte强转成Struct指针就行
内存映射原理如下图所示
![image](https://github.com/yuadsl3010/heyicache-benchmark/blob/master/img/heyicache.svg)

## 使用限制
如此巨大的性能提升，but at what cost?
### 1. 作为value的struct有类型限制
value必须是*struct，且struct中的map成员无法被cache且会被强制指向nil
1. 类型固定为*struct：更方便为其提供内存映射函数的代码（接入例子的第二节，可以为任意struct生成内存映射函数，无需人工干预）
2. 不支持map成员：golang map所占用的内存并不连续，极其碎片化的分配方式导致无法用一段连续内存进行存储，推荐的实现方式是采用数组进行存储（如果有更好的内存管理方式，也欢迎一起探讨）
### 2. value是只读的
由于struct在内存映射后，所有的指针都指向那一段分配好的连续内存，所以哪怕是修改的string，也会导致下次get的string指向已被gc回收的内存，触发panic
所以value必须是只读的
tips: 我自己在业务实践的时候，也会去修改其中的某个静态变量（uint64、bool这种），因为这种变量存在在连续内存中，不会被gc回收，算是一个比较hack的使用方式。但用户在修改前，一定要清楚的知道自己在修改什么，否则会引发panic
### 3. 极少量的写错误、少量的内存限制超出和稍高的数据过期概率
由于内存映射的关系，heyicache的淘汰最小单位是一个segment（与freecache一样，有256个segment，例如cache总空间是256MB，那么一次淘汰就是1MB）
因为无法知道这个segment上哪些数据正在被访问，所以当buffer写满的时候，只能新创建一个buffer，老buffer确认无法访问之后再回收
这样的特性导致：
当老buffer还未被回收的时候，无法创建新buffer，此时写入会失败（默认是3份buffer，也就极端情况下一个buffer会膨胀到预设的3倍；正常情况下，回收及时并不会超过预设值）
同理，一次性回收一整个segment，必然会导致一些数据被提前淘汰，也会带来cache率的些许下降
根据我自己的业务实践，相比性能的提升，cache率少许下降带来的损失可以忽略不计
### 4. 需要在get的数据不再访问后，主动进行lease的归还
如果你是grpc服务，那么最好增加一个中间件，确保respone已经封包后，调用heyicache.GetLeaseCtx(ctx).Done()进行归还（见接入例子第3部分）
否则heyicache无法判断buffer是否还在使用，在buffer写满的情况下就无法创建新的空间

## 使用建议
绝大部分场景按照接入例子可以进行快速接入，但建议定时增加heyicache的数据上报，可以帮助你快速分析是否应该增减内存或者调整数据访问方式

有任何疑问或者建议，也欢迎一起交流和讨论
