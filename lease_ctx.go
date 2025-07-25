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
	leases map[string]*Lease
}

type Lease struct {
	keeps *typeLease
	cache *Cache
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

func (leaseCtx *LeaseCtx) GetLease(cache *Cache) *Lease {
	if leaseCtx == nil || cache == nil {
		return nil
	}
	if _, ok := leaseCtx.leases[cache.Name]; !ok {
		leaseCtx.leases[cache.Name] = &Lease{
			cache: cache,
			keeps: keepsPool.Get().(*typeLease),
		}
	}
	return leaseCtx.leases[cache.Name]
}

func (leaseCtx *LeaseCtx) Done() {
	if leaseCtx == nil {
		return
	}

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
				lease.cache.segments[segID].update(int32(block), -k)
				lease.cache.locks[segID].Unlock()
			}
		}
		// 归还 keeps 到对象池
		// 快速将 lease.keeps 全部置为 0，采用内存拷贝
		*lease.keeps = keepsNew
		keepsPool.Put(lease.keeps)
		lease.keeps = nil
	}
}
