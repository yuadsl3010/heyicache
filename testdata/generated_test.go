package generatedtest

// skill_name: development; plugin_name: development; skill_branch: main;
// skill_commit: 125eeb496930a08d171acf2dbc7f65f1aa3c4f74;
// skill_updated_at: 2026-09-30T15:53:35+08:00; skill_version_status: unknown

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"unsafe"

	hc "github.com/yuadsl3010/heyicache"
	"github.com/yuadsl3010/heyicache/internal/testmodel"
)

func check(t *testing.T, got *testmodel.Record, deep bool) {
	t.Helper()
	if !got.OmittedZero() {
		t.Fatal("omitted fields are not zero")
	}
	if got.ID != 17 || got.Name != "original" || string(got.Data) != "payload" || got.Inline.Value != 7 || got.Inline.Label != "nested" {
		t.Fatal("public fields changed")
	}
	if !got.Inline.PrivateZero() || !got.Array[0].PrivateZero() || !got.Matrix[1][1].PrivateZero() {
		t.Fatal("inline private fields copied")
	}
	if got.Numbers != [2]uint64{3, 4} || got.Names != [2]string{"one", "two"} || string(got.ArraySlices[0]) != "abc" || got.ArraySlices[1] != nil {
		t.Fatal("array values changed")
	}
	if deep {
		if !got.Child.PrivateZero() || !got.Pointers[0].PrivateZero() || !got.ArrayPointers[0].PrivateZero() || !got.Children[0].PrivateZero() {
			t.Fatal("nested private fields copied")
		}
		if got.Pointers[1] != nil || got.ArrayPointers[1] != nil {
			t.Fatal("nil child changed")
		}
	}
}

func TestCopies(t *testing.T) {
	fn := HeyiCacheFnRecordIfc_
	src := testmodel.New()
	size := fn.Size(src, true)
	buf := bytes.Repeat([]byte{0xff}, int(size))
	value, used := fn.Set(src, buf, true)
	if used != size {
		t.Fatalf("size=%d used=%d", size, used)
	}
	got := value.(*testmodel.Record)
	check(t, got, true)
	if src.PrivateZero() || src.Child.PrivateZero() {
		t.Fatal("source modified")
	}
	// Private payloads must not contribute any bytes to the arena allocation.
	clean := new(testmodel.Record)
	fn.DeepCopy(src, clean)
	if fn.Size(clean, true) != size {
		t.Fatal("private fields contribute to Size")
	}
	shallow := testmodel.New() // deliberately dirty destination
	fn.ShallowCopy(src, shallow)
	check(t, shallow, false)
	if shallow.Child != src.Child || &shallow.Data[0] != &src.Data[0] {
		t.Fatal("shallow copy unexpectedly deep")
	}
	fn.DeepCopy(src, clean)
	check(t, clean, true)
	if clean.Child == src.Child || &clean.Data[0] == &src.Data[0] {
		t.Fatal("deep copy aliases source")
	}
	fn.ShallowCopy(shallow, shallow)
	check(t, shallow, false)
	fn.DeepCopy(clean, clean)
	check(t, clean, true)
	// Set(false) may be called on an arena-resident inline object.
	inPlace := testmodel.New()
	extra := make([]byte, fn.Size(inPlace, false))
	_, used = fn.Set(inPlace, extra, false)
	if used != int32(len(extra)) {
		t.Fatal("Set(false) size mismatch")
	}
	check(t, inPlace, true)
	fn.Put(testmodel.New())
	pooled := fn.New(true).(*testmodel.Record)
	if !pooled.OmittedZero() || pooled.ID != 0 {
		t.Fatal("pool returned stale fields")
	}
	fn.ShallowCopy(got, pooled)
	check(t, pooled, true)
	fn.Put(pooled)
	// Mutating/releasing source storage must not affect the cache.
	src.Data[0] = '!'
	src.Child.Value = 999
	src.Children[0].Value = 999
	src = nil
	runtime.GC()
	check(t, fn.Get(buf).(*testmodel.Record), true)
	runtime.KeepAlive(buf)
}

func TestOnlyPrivate(t *testing.T) {
	fn := HeyiCacheFnOnlyPrivateIfc_
	src := testmodel.NewOnlyPrivate()
	buf := bytes.Repeat([]byte{255}, int(fn.Size(src, true)))
	got, _ := fn.Set(src, buf, true)
	if !got.(*testmodel.OnlyPrivate).Zero() || src.Zero() {
		t.Fatal("private-only object was not zeroed or source changed")
	}
}

func TestCacheRoundTrip(t *testing.T) {
	cache, err := hc.NewCache(hc.DefaultConfig(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("record")
	src := testmodel.New()
	if err := cache.Set(key, src, HeyiCacheFnRecordIfc_, 60); err != nil {
		t.Fatal(err)
	}
	src.Data[0] = '!'
	src.Child.Value = 999
	src = nil
	runtime.GC()
	readers := []struct {
		name string
		get  func(*hc.Lease, []byte, hc.HeyiCacheFnIfc) (interface{}, error)
	}{
		{"zero", cache.GetZeroCopy}, {"shallow", cache.Get},
		{"pool", cache.GetFromObjPool}, {"deep", cache.GetDeepCopy},
	}
	for _, reader := range readers {
		t.Run(reader.name, func(t *testing.T) {
			t.Parallel()
			ctx := hc.GetLeaseCtx(hc.NewLeaseCtx(context.Background()))
			defer ctx.Done()
			value, err := reader.get(ctx.GetLease(cache), key, HeyiCacheFnRecordIfc_)
			if err != nil {
				t.Fatal(err)
			}
			check(t, value.(*testmodel.Record), true)
		})
	}
}

func BenchmarkCodec(b *testing.B) {
	fn := HeyiCacheFnRecordIfc_
	src := testmodel.New()
	buf := make([]byte, fn.Size(src, true))
	fn.Set(src, buf, true)
	b.Run("Set", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkSink, _ = fn.Set(src, buf, true)
		}
	})
	b.Run("GetZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkSink = fn.Get(buf)
		}
	})
	b.Run("DeepCopy", func(b *testing.B) {
		dst := new(testmodel.Record)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			fn.DeepCopy(src, dst)
		}
		benchmarkSink = dst
	})
}

var benchmarkSink any

func BenchmarkProjection(b *testing.B) {
	src := testmodel.New()
	dst := new(testmodel.Record)
	b.Run("OldRawCopy", func(b *testing.B) {
		n := int(unsafe.Sizeof(*src))
		s := unsafe.Slice((*byte)(unsafe.Pointer(src)), n)
		d := unsafe.Slice((*byte)(unsafe.Pointer(dst)), n)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			copy(d, s)
		}
		benchmarkSink = dst
	})
	b.Run("PublicProjection", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			HeyiCacheFnRecordIfc_.ShallowCopy(src, dst)
		}
		benchmarkSink = dst
	})
}

// Keep the public cache API in the compilation contract.
var _ hc.HeyiCacheFnIfc = HeyiCacheFnRecordIfc_
