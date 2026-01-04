package heyicache

import (
	"context"
	"fmt"
	"sync"
)

type typeLease [segCount][blockCount]int32

var (
	ErrNilLeaseCtx = fmt.Errorf("lease context is nil")
	leaseCtxKey    = "arena_cache_lease"
	keepsPool      = newKeepsPool()
	keepsNew       = typeLease{}
)

// 对象池用于复用 keeps 数组，减少内存分配
func newKeepsPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			return new(typeLease)
		},
	}
}

type LeaseCtx struct {
	mu     sync.RWMutex
	leases map[string]*Lease
}

type Lease struct {
	keeps *typeLease
	cache *Cache
	mutex sync.Mutex
	objs  map[HeyiCacheFnIfc][]interface{}
}

func NewLeaseCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, leaseCtxKey, &LeaseCtx{
		leases: make(map[string]*Lease),
	})
}

func GetLeaseCtx(ctx context.Context) *LeaseCtx {
	if ctx == nil {
		return nil
	}

	leaseCtx, ok := ctx.Value(leaseCtxKey).(*LeaseCtx)
	if !ok {
		return nil
	}

	return leaseCtx
}

// concurrently safe
func (leaseCtx *LeaseCtx) GetLease(cache *Cache) *Lease {
	if leaseCtx == nil || cache == nil {
		return nil
	}

	// 先尝试读取，避免不必要的写操作
	leaseCtx.mu.RLock()
	lease, ok := leaseCtx.leases[cache.leaseName]
	leaseCtx.mu.RUnlock()
	if ok {
		return lease
	}

	// 使用写锁创建新的 Lease
	leaseCtx.mu.Lock()
	defer leaseCtx.mu.Unlock()

	// 双重检查，防止并发创建
	if lease, ok := leaseCtx.leases[cache.leaseName]; ok {
		return lease
	}

	newLease := &Lease{
		cache: cache,
		keeps: keepsPool.Get().(*typeLease),
	}

	leaseCtx.leases[cache.leaseName] = newLease
	return newLease
}

// concurrently unsafe because it should be called only once when the context is done
func (leaseCtx *LeaseCtx) Done() {
	if leaseCtx == nil {
		return
	}

	leaseCtx.mu.RLock()
	defer leaseCtx.mu.RUnlock()

	for _, lease := range leaseCtx.leases {
		if lease == nil {
			continue
		}
		for segID, vs := range *(lease.keeps) {
			for block, k := range vs {
				if k <= 0 {
					continue
				}
				lease.cache.locks[segID].Lock()
				seg := &lease.cache.segments[segID]
				seg.bufs[block].used -= k
				if seg.bufs[block].used == 0 && seg.isInEviction(int32(block)) {
					seg.eviction()
				}
				lease.cache.locks[segID].Unlock()
			}
		}
		// 归还 keeps 到对象池
		// 快速将 lease.keeps 全部置为 0，采用内存拷贝
		*lease.keeps = keepsNew
		keepsPool.Put(lease.keeps)
		lease.keeps = nil

		// 归还 objs 到对象池
		for fn, objs := range lease.objs {
			for _, obj := range objs {
				fn.Put(obj)
			}
		}
		lease.objs = nil
	}
}
