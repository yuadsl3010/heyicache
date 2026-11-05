package codec

// skill_name: development; plugin_name: development; skill_branch: main;
// skill_commit: 125eeb496930a08d171acf2dbc7f65f1aa3c4f74;
// skill_updated_at: 2026-09-30T15:53:35+08:00; skill_version_status: unknown

import (
	"bytes"
	"context"
	hc "github.com/yuadsl3010/heyicache"
	"github.com/yuadsl3010/heyicache/litetest/model"
	"runtime"
	"testing"
)

func TestLiteCache(t *testing.T) {
	child := &model.Child{Name: "nested"}
	clean := &model.Record{Id: 7, Child: child, Children: []*model.Child{child}}
	expected, err := clean.MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	childWire, err := child.MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	child.Reset()
	if err := child.UnmarshalVT(append(childWire, 0xa0, 6, 1)); err != nil {
		t.Fatal(err)
	}
	wire, err := clean.MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	wire = append(wire, 0xa0, 6, 2)
	src := new(model.Record)
	if err := src.UnmarshalVT(wire); err != nil {
		t.Fatal(err)
	}
	cache, err := hc.NewCache(hc.DefaultConfig(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Set([]byte("key"), src, HeyiCacheFnRecordIfc_, 60); err != nil {
		t.Fatal(err)
	}
	unchanged, err := src.MarshalVT()
	if err != nil || !bytes.Equal(unchanged, wire) {
		t.Fatal("Set changed source/unknown fields")
	}
	src = nil
	runtime.GC()
	ctx := hc.GetLeaseCtx(hc.NewLeaseCtx(context.Background()))
	defer ctx.Done()
	value, err := cache.GetFromObjPool(ctx.GetLease(cache), []byte("key"), HeyiCacheFnRecordIfc_)
	if err != nil {
		t.Fatal(err)
	}
	got, err := value.(*model.Record).MarshalVT()
	if err != nil || !bytes.Equal(got, expected) {
		t.Fatalf("known fields changed or unknown fields retained: %x want %x err %v", got, expected, err)
	}
}
