// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"errors"
	"math"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage/experimental"
	"google.golang.org/api/option"
)

const testBlockSize = 1024

// newTestBufferPool returns a pool with small blocks and an idle timeout long
// enough that the cleaner never ticks; tests drive shrinkIfIdle directly.
func newTestBufferPool(t *testing.T) *defaultBufferPool {
	t.Helper()
	p := newDefaultBufferPool()
	p.blockSize = testBlockSize
	p.idleTimeout = time.Hour
	t.Cleanup(p.close)
	return p
}

// waitFor polls cond until it is true or the deadline expires.
func waitFor(tb testing.TB, desc string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out waiting for %s", desc)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitIdle waits for any background refill to finish.
func waitIdle(tb testing.TB, p *defaultBufferPool) {
	tb.Helper()
	waitFor(tb, "refill to finish", func() bool { return !p.refilling.Load() })
}

func (p *defaultBufferPool) isCleanerRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cleanerRunning
}

func (p *defaultBufferPool) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func TestDefaultBufferPoolGetSizes(t *testing.T) {
	p := newTestBufferPool(t)
	for _, tc := range []struct {
		maxSize int
		wantLen int
	}{
		{1, 1},
		{testBlockSize / 2, testBlockSize / 2},
		{testBlockSize, testBlockSize},
		{3 * testBlockSize, testBlockSize},
	} {
		b, err := p.Get(tc.maxSize)
		if err != nil {
			t.Fatalf("Get(%d): %v", tc.maxSize, err)
		}
		if len(b) != tc.wantLen || cap(b) != testBlockSize {
			t.Errorf("Get(%d): len=%d cap=%d, want len=%d cap=%d", tc.maxSize, len(b), cap(b), tc.wantLen, testBlockSize)
		}
		b2, ok, err := p.TryGet(tc.maxSize)
		if !ok || err != nil {
			t.Fatalf("TryGet(%d) = %v, %v; want true, nil", tc.maxSize, ok, err)
		}
		if len(b2) != tc.wantLen || cap(b2) != testBlockSize {
			t.Errorf("TryGet(%d): len=%d cap=%d, want len=%d cap=%d", tc.maxSize, len(b2), cap(b2), tc.wantLen, testBlockSize)
		}
		p.Put(b)
		p.Put(b2)
		waitIdle(t, p)
	}
}

func TestDefaultBufferPoolInvalidSize(t *testing.T) {
	p := newTestBufferPool(t)
	for _, n := range []int{0, -1} {
		if _, err := p.Get(n); !errors.Is(err, errInvalidBufferSize) {
			t.Errorf("Get(%d) err = %v, want %v", n, err, errInvalidBufferSize)
		}
		if _, ok, err := p.TryGet(n); ok || !errors.Is(err, errInvalidBufferSize) {
			t.Errorf("TryGet(%d) = %v, %v; want false, %v", n, ok, err, errInvalidBufferSize)
		}
	}
}

// The default pool has no memory limit, so TryGet succeeds even on a cold
// pool and never throttles parallel segments.
func TestDefaultBufferPoolTryGetNeverRefuses(t *testing.T) {
	p := newTestBufferPool(t)
	for i := 0; i < 10; i++ {
		if _, ok, err := p.TryGet(4 * testBlockSize); !ok || err != nil {
			t.Fatalf("TryGet #%d = %v, %v; want true, nil", i, ok, err)
		}
	}
}

func TestDefaultBufferPoolBlocksFor(t *testing.T) {
	p := newTestBufferPool(t)
	for _, tc := range []struct {
		size int
		want int
	}{
		{1, 1},
		{testBlockSize - 1, 1},
		{testBlockSize, 1},
		{testBlockSize + 1, 2},
		{3 * testBlockSize, 3},
		// Must not overflow for very large sizes.
		{math.MaxInt, 1 + (math.MaxInt-1)/testBlockSize},
	} {
		if got := p.blocksFor(tc.size); got != tc.want {
			t.Errorf("blocksFor(%d) = %d, want %d", tc.size, got, tc.want)
		}
	}
}

// Refill pre-allocates only the spare blocks a part still needs beyond the
// block just handed out, capped at poolMaxRefillBlocks.
func TestDefaultBufferPoolRefill(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxSize    int
		wantCached int
	}{
		{"single-block part", testBlockSize, 0},
		{"two-block part", 2 * testBlockSize, 1},
		{"three-block part", 3 * testBlockSize, 2},
		{"capped", 10 * testBlockSize, poolMaxRefillBlocks},
		{"huge maxSize", math.MaxInt, poolMaxRefillBlocks},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestBufferPool(t)
			b, err := p.Get(tc.maxSize)
			if err != nil {
				t.Fatal(err)
			}
			if len(b) != min(tc.maxSize, testBlockSize) {
				t.Errorf("Get(%d) len = %d", tc.maxSize, len(b))
			}
			waitIdle(t, p)
			if got := p.cached(); got != tc.wantCached {
				t.Errorf("cached = %d, want %d", got, tc.wantCached)
			}
			// No refill once enough spare blocks are cached.
			if tc.wantCached > 0 {
				p.Put(b)
				if _, err := p.Get(tc.maxSize); err != nil {
					t.Fatal(err)
				}
				if p.refilling.Load() {
					t.Error("refill started although enough blocks were cached")
				}
			}
		})
	}
}

// Put racing with close must never leave a block cached in a closed pool.
func TestDefaultBufferPoolPutCloseRace(t *testing.T) {
	const (
		rounds  = 50
		workers = 16
	)
	for r := 0; r < rounds; r++ {
		p := newDefaultBufferPool()
		p.blockSize = testBlockSize
		held := make([][]byte, workers)
		for i := range held {
			held[i], _ = p.Get(testBlockSize)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, b := range held {
			wg.Add(1)
			go func(b []byte) {
				defer wg.Done()
				<-start
				p.Put(b)
			}(b)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p.close()
		}()
		close(start)
		wg.Wait()

		if got := p.cached(); got != 0 {
			t.Fatalf("round %d: %d blocks cached in closed pool", r, got)
		}
	}
}

// Put caches the block for reuse, and the cache hands out the most recently
// returned block first (LIFO).
func TestDefaultBufferPoolReuse(t *testing.T) {
	p := newTestBufferPool(t)
	b1, _ := p.Get(testBlockSize)
	b2, _ := p.Get(testBlockSize / 2)
	p.Put(b1)
	// Put accepts the exact (possibly short) slice returned by Get.
	p.Put(b2)

	got, _ := p.Get(testBlockSize)
	if &got[0] != &b2[0] || len(got) != testBlockSize {
		t.Errorf("Get did not return the most recently Put block at full length (len=%d)", len(got))
	}
	got, _ = p.Get(testBlockSize)
	if &got[0] != &b1[0] {
		t.Error("second Get did not reuse the earlier Put block")
	}
}

func TestDefaultBufferPoolPutForeignBuffer(t *testing.T) {
	p := newTestBufferPool(t)
	p.Put(make([]byte, testBlockSize+1))
	p.Put(make([]byte, 10))
	p.Put(nil)
	if got := p.cached(); got != 0 {
		t.Errorf("foreign buffers were cached: cached=%d", got)
	}
}

func TestDefaultBufferPoolStagedEviction(t *testing.T) {
	p := newTestBufferPool(t)
	// Single-block Gets start no refill, so exactly 7 blocks get cached.
	var bufs [][]byte
	for i := 0; i < 7; i++ {
		b, _ := p.Get(testBlockSize)
		bufs = append(bufs, b)
	}
	for _, b := range bufs {
		p.Put(b)
	}
	if got := p.cached(); got != 7 {
		t.Fatalf("cached = %d, want 7", got)
	}

	steps := []struct {
		activity   bool
		wantCached int
		wantStop   bool
		desc       string
	}{
		{false, 7, false, "tick after Put: pool was active, no eviction"},
		{false, 3, false, "idle tick: halve"},
		{true, 3, false, "tick after activity: no eviction"},
		{false, 1, false, "idle tick: halve again"},
		{false, 0, true, "idle tick: empty, cleaner stops"},
	}
	for _, s := range steps {
		if s.activity {
			b, _ := p.Get(testBlockSize)
			p.Put(b)
		}
		stopped := p.shrinkIfIdle()
		if got := p.cached(); got != s.wantCached || stopped != s.wantStop {
			t.Fatalf("%s: cached=%d stopped=%v, want cached=%d stopped=%v", s.desc, got, stopped, s.wantCached, s.wantStop)
		}
	}
}

func TestDefaultBufferPoolCleanerLifecycle(t *testing.T) {
	p := newDefaultBufferPool()
	p.blockSize = testBlockSize
	p.idleTimeout = time.Millisecond
	defer p.close()

	if p.isCleanerRunning() {
		t.Fatal("cleaner running before any block was cached")
	}
	b, _ := p.Get(testBlockSize)
	waitIdle(t, p)
	p.Put(b)
	if !p.isCleanerRunning() {
		t.Fatal("cleaner not started after a block was cached")
	}
	waitFor(t, "cleaner to drain the pool and stop", func() bool {
		return p.cached() == 0 && !p.isCleanerRunning()
	})

	// The next cached block restarts the cleaner.
	b, _ = p.Get(testBlockSize)
	p.Put(b)
	if !p.isCleanerRunning() {
		t.Fatal("cleaner not restarted after a block was cached")
	}
}

func TestDefaultBufferPoolClose(t *testing.T) {
	p := newTestBufferPool(t)
	b1, _ := p.Get(testBlockSize)
	b2, _ := p.Get(testBlockSize)
	p.Put(b1)
	p.close()
	// close must be idempotent.
	p.close()

	if got := p.cached(); got != 0 {
		t.Errorf("cached = %d after close, want 0", got)
	}
	// Buffers returned after close are dropped and must not restart the
	// cleaner goroutine.
	p.Put(b2)
	if got := p.cached(); got != 0 {
		t.Errorf("cached = %d after Put on a closed pool, want 0", got)
	}
	if p.isCleanerRunning() {
		t.Error("cleaner restarted by Put after close")
	}
	// Get and TryGet still work but must not start a refill.
	if _, ok, err := p.TryGet(4 * testBlockSize); !ok || err != nil {
		t.Errorf("TryGet after close = %v, %v; want true, nil", ok, err)
	}
	if _, err := p.Get(4 * testBlockSize); err != nil {
		t.Errorf("Get after close: %v", err)
	}
	if p.refilling.Load() {
		t.Error("refill started after close")
	}
}

// Many writers assembling parts while the cleaner stops and restarts as often
// as possible. Once they finish, the cleaner must drain the pool; a block
// stranded without a running cleaner would keep it from emptying.
func TestDefaultBufferPoolConcurrency(t *testing.T) {
	const (
		workers = 32
		iters   = 500
	)
	p := newDefaultBufferPool()
	p.blockSize = testBlockSize
	p.idleTimeout = time.Millisecond
	defer p.close()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				// Mimic part assembly: one Get, then TryGet for the rest.
				first, err := p.Get(4 * testBlockSize)
				if err != nil {
					t.Error(err)
					return
				}
				held := [][]byte{first}
				for j := 0; j < 3; j++ {
					b, ok, err := p.TryGet(4 * testBlockSize)
					if !ok || err != nil {
						t.Errorf("TryGet = %v, %v; want true, nil", ok, err)
						return
					}
					held = append(held, b)
				}
				for _, b := range held {
					b[0], b[len(b)-1] = byte(w), byte(i)
					p.Put(b)
				}
			}
		}(w)
	}
	wg.Wait()

	waitFor(t, "cleaner to drain the pool", func() bool {
		return p.cached() == 0 && !p.refilling.Load() && !p.isCleanerRunning()
	})
}

// Steady-state Get/Put must not allocate. Benchmarks do not run in CI, so this
// asserts it deterministically.
func TestDefaultBufferPoolZeroAllocs(t *testing.T) {
	p := newTestBufferPool(t)
	b, _ := p.Get(testBlockSize)
	p.Put(b)
	allocs := testing.AllocsPerRun(1000, func() {
		b, _ := p.Get(testBlockSize)
		p.Put(b)
	})
	if allocs != 0 {
		t.Errorf("Get+Put allocs = %v, want 0", allocs)
	}
}

// prewarm caches n blocks in p and waits for any refill to finish.
func prewarm(tb testing.TB, p *defaultBufferPool, n int) {
	tb.Helper()
	bufs := make([][]byte, n)
	for i := range bufs {
		bufs[i], _ = p.Get(p.blockSize)
	}
	for _, b := range bufs {
		p.Put(b)
	}
	waitIdle(tb, p)
}

func BenchmarkDefaultBufferPoolSteadyState(b *testing.B) {
	p := newDefaultBufferPool()
	defer p.close()
	// A multi-block part, so that refills are exercised.
	const blocksPerPart = 4
	const partSize = blocksPerPart * poolBlockSize
	// Cache the blocks of one part plus the spares a refill keeps ready, so
	// the timed loop never needs to grow the pool.
	prewarm(b, p, blocksPerPart+poolMaxRefillBlocks)

	held := make([][]byte, 0, blocksPerPart)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ := p.Get(partSize)
		held = append(held, buf)
		for len(held) < cap(held) {
			buf, _, _ := p.TryGet(partSize)
			held = append(held, buf)
		}
		for _, buf := range held {
			p.Put(buf)
		}
		held = held[:0]
	}
}

func BenchmarkDefaultBufferPoolParallel(b *testing.B) {
	p := newDefaultBufferPool()
	defer p.close()
	// Warm the pool with enough blocks for every parallel goroutine so the
	// timed loop measures steady state rather than pool growth.
	prewarm(b, p, 2*runtime.GOMAXPROCS(0))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf, _ := p.Get(poolBlockSize)
			p.Put(buf)
		}
	})
}

func TestGRPCClientBufferPool(t *testing.T) {
	ctx := context.Background()
	baseOpts := []option.ClientOption{
		option.WithoutAuthentication(),
		option.WithEndpoint("localhost:0"),
		WithDisabledClientMetrics(),
	}

	t.Run("default", func(t *testing.T) {
		c, err := newGRPCStorageClient(ctx, withClientOptions(baseOpts...))
		if err != nil {
			t.Fatal(err)
		}
		pool, ok := c.bufferPool.(*defaultBufferPool)
		if !ok {
			t.Fatalf("bufferPool = %T, want *defaultBufferPool", c.bufferPool)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if !pool.isClosed() {
			t.Error("default pool not closed with the client")
		}
	})

	t.Run("custom", func(t *testing.T) {
		custom := &struct{ experimental.BufferPool }{}
		opts := append(slices.Clone(baseOpts), experimental.WithBufferPool(custom))
		c, err := newGRPCStorageClient(ctx, withClientOptions(opts...))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if c.bufferPool != experimental.BufferPool(custom) {
			t.Error("bufferPool is not the custom pool")
		}
	})
}
