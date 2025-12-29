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
	leases sync.Map // map[string]*Lease
}

type Lease struct {
	keeps *typeLease
	cache *Cache
}

func NewLeaseCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, leaseCtxKey, &LeaseCtx{})
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
	if value, ok := leaseCtx.leases.Load(cache.leaseName); ok {
		return value.(*Lease)
	}

	// 使用 LoadOrStore 保证原子性，避免重复创建
	newLease := &Lease{
		cache: cache,
		keeps: keepsPool.Get().(*typeLease),
	}

	actual, _ := leaseCtx.leases.LoadOrStore(cache.leaseName, newLease)
	lease := actual.(*Lease)

	// 如果 LoadOrStore 返回了已存在的值，需要归还新创建的 keeps
	if lease != newLease {
		// keepsPool.Put(newLease.keeps)
	}

	return lease
}

// concurrently unsafe because it should be called only once when the context is done
func (leaseCtx *LeaseCtx) Done() {
	if leaseCtx == nil {
		return
	}

	leaseCtx.leases.Range(func(key, value interface{}) bool {
		lease := value.(*Lease)
		if lease == nil {
			return true // continue iteration
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
		// keepsPool.Put(lease.keeps)
		lease.keeps = nil

		return true // continue iteration
	})
}
