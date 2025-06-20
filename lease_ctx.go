package heyicache

import (
	"context"
	"fmt"
)

var (
	ErrNilLeaseCtx = fmt.Errorf("lease context is nil")
	leaseCtxKey    = "arena_cache_lease"
)

type LeaseCtx struct {
	leases map[string]*Lease
}

type Lease struct {
	keeps [segCount][versionCount]int64
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
			keeps: [segCount][versionCount]int64{},
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

		for segID, vs := range lease.keeps {
			for version, k := range vs {
				if k <= 0 {
					continue
				}

				lease.cache.locks[segID].Lock()
				segment := &lease.cache.segments[segID]
				if int32(version) == segment.version {
					segment.used -= k
				} else {
					segment.lastUsed -= k
				}
				lease.cache.locks[segID].Unlock()
			}
		}
	}
}
