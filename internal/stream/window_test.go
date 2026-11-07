package stream

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// checkPlan holds a plan to Telegram's rules and to covering start..end exactly once, in order.
func checkPlan(t *testing.T, start, end, largest int64) []blockSpec {
	t.Helper()
	blocks := planBlocks(start, end, largest)
	if len(blocks) == 0 {
		t.Fatalf("no blocks for %d..%d", start, end)
	}
	if blocks[0].offset > start || blocks[len(blocks)-1].offset+blocks[len(blocks)-1].size <= end {
		t.Fatalf("%d..%d not covered: %v ... %v", start, end, blocks[0], blocks[len(blocks)-1])
	}
	for i, b := range blocks {
		if b.size < minBlock || b.size > maxBlock || b.size&(b.size-1) != 0 || maxBlock%b.size != 0 {
			t.Fatalf("block %d: size %d breaks Telegram's limit rules", i, b.size)
		}
		if b.offset%minBlock != 0 || b.offset%b.size != 0 || b.offset/maxBlock != (b.offset+b.size-1)/maxBlock {
			t.Fatalf("block %d: offset %d size %d breaks Telegram's offset rules", i, b.offset, b.size)
		}
		if b.size > largest && largest >= minBlock {
			t.Fatalf("block %d: size %d over the largest %d", i, b.size, largest)
		}
		if i > 0 && b.offset != blocks[i-1].offset+blocks[i-1].size {
			t.Fatalf("block %d: gap or overlap at %d", i, b.offset)
		}
	}
	return blocks
}

func TestPlanBlocks(t *testing.T) {
	// A whole 3 GB file: small first, then 1 MB blocks.
	blocks := checkPlan(t, 0, 3<<30-1, 1<<20)
	if blocks[0].size != 64<<10 {
		t.Fatalf("first block %d, want 64 KB", blocks[0].size)
	}
	if blocks[len(blocks)-1].size != 1<<20 || blocks[20].size != 1<<20 {
		t.Fatal("not 1 MB blocks after the start")
	}
	// A seek to an odd place in the middle of a large file.
	checkPlan(t, 1234567891, 3<<30-1, 1<<20)
	// The tail of a file, as a player reads an index there.
	tail := checkPlan(t, 3<<30-37000, 3<<30-1, 64<<10)
	var fetched int64
	for _, b := range tail {
		fetched += b.size
	}
	if fetched > 37000+2*minBlock {
		t.Fatalf("a 37 KB read fetches %d bytes", fetched)
	}
	// Tiny and edge ranges.
	checkPlan(t, 0, 0, 64<<10)
	checkPlan(t, 4095, 4096, 1<<20)
	checkPlan(t, 5, 1<<20, 1<<20)
	checkPlan(t, 0, 10<<20, 3<<20)   // larger than Telegram allows: 1 MB
	checkPlan(t, 0, 10<<20, 300<<10) // not a power of two: 256 KB
	checkPlan(t, 0, 10<<20, 0)       // nonsense: 4 KB
	r := rand.New(rand.NewSource(1))
	for range 2000 {
		start := r.Int63n(8 << 30)
		end := start + r.Int63n(64<<20)
		checkPlan(t, start, end, int64(64<<10)<<r.Intn(5))
	}
}

func TestTrimBlock(t *testing.T) {
	data := []byte("0123456789")
	b := blockSpec{offset: 100, size: 16}
	cases := []struct {
		start, end int64
		want       string
	}{
		{100, 109, "0123456789"},
		{103, 109, "3456789"},
		{100, 104, "01234"},
		{103, 105, "345"},
		{50, 200, "0123456789"},
		{112, 200, ""}, // past what Telegram sent
	}
	for _, c := range cases {
		if got := string(trimBlock(b, data, c.start, c.end)); got != c.want {
			t.Errorf("trim %d..%d = %q, want %q", c.start, c.end, got, c.want)
		}
	}
}

// fakeFile answers like Telegram for a file of n bytes, after a random delay, counting requests in flight.
type fakeFile struct {
	data          []byte
	inFlight, max atomic.Int32
	failAt        int64
}

func (f *fakeFile) fetch(ctx context.Context, b blockSpec) ([]byte, error) {
	now := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		old := f.max.Load()
		if now <= old || f.max.CompareAndSwap(old, now) {
			break
		}
	}
	select {
	case <-time.After(time.Duration(rand.Intn(3000)) * time.Microsecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if f.failAt >= 0 && b.offset >= f.failAt {
		return nil, errors.New("telegram failed")
	}
	if b.offset >= int64(len(f.data)) {
		return nil, nil
	}
	return f.data[b.offset:min(int64(len(f.data)), b.offset+b.size)], nil
}

func TestFetchInOrder(t *testing.T) {
	f := &fakeFile{data: make([]byte, 9<<20+12345), failAt: -1}
	rand.New(rand.NewSource(2)).Read(f.data)
	for _, rng := range [][2]int64{{0, int64(len(f.data)) - 1}, {777777, 5 << 20}, {int64(len(f.data)) - 5000, int64(len(f.data)) - 1}} {
		start, end := rng[0], rng[1]
		blocks := planBlocks(start, end, 1<<20)
		var got bytes.Buffer
		err := fetchInOrder(context.Background(), blocks, 6, f.fetch, func(i int, data []byte) error {
			got.Write(trimBlock(blocks[i], data, start, end))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Bytes(), f.data[start:end+1]) {
			t.Fatalf("range %d..%d: wrong bytes (%d of %d)", start, end, got.Len(), end-start+1)
		}
		if m := f.max.Load(); m > 6 {
			t.Fatalf("%d requests at once, over 6", m)
		}
	}
}

func TestFetchInOrderStops(t *testing.T) {
	// A failing block stops it, and nothing after it is delivered.
	f := &fakeFile{data: make([]byte, 8<<20), failAt: 3 << 20}
	blocks := planBlocks(0, 8<<20-1, 1<<20)
	var last int64
	err := fetchInOrder(context.Background(), blocks, 4, f.fetch, func(i int, data []byte) error {
		last = blocks[i].offset
		return nil
	})
	if err == nil || last >= 3<<20 {
		t.Fatalf("err %v, delivered up to %d", err, last)
	}
	// The player leaving (deliver failing) stops it too.
	stop := errors.New("player left")
	n := 0
	err = fetchInOrder(context.Background(), blocks, 4, (&fakeFile{data: make([]byte, 8<<20), failAt: -1}).fetch, func(int, []byte) error {
		n++
		if n == 3 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || n != 3 {
		t.Fatalf("err %v after %d blocks", err, n)
	}
	// A cancelled context ends it.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fetchInOrder(ctx, blocks, 4, (&fakeFile{data: make([]byte, 8<<20), failAt: -1}).fetch, func(int, []byte) error { return nil }); err == nil {
		t.Fatal("ran on after its context ended")
	}
}
