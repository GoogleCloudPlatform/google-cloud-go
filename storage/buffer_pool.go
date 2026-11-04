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
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// poolBlockSize is the size of every block vended by the default buffer
	// pool. GCS parallel upload parts are multiples of 32 MiB, so each
	// minimum-sized part is backed by a single block.
	poolBlockSize = 32 << 20

	// poolMaxRefillBlocks caps how many spare blocks a background refill
	// allocates (64 MiB). It does not limit Get, which always succeeds.
	poolMaxRefillBlocks = 2

	// poolIdleTimeout is how often the cleaner runs. Each run that follows
	// a full interval without Get or Put evicts half of the cached blocks.
	poolIdleTimeout = 30 * time.Second
)

var errInvalidBufferSize = errors.New("storage: buffer pool: maxSize must be positive")

// defaultBufferPool is the BufferPool used for parallel uploads when the
// application does not provide one through experimental.WithBufferPool.
//
// It vends uniform 32 MiB blocks shared across all writers of a client. The
// pool has no memory limit; memory in use is bounded by the writers'
// PartSize and Concurrency settings. Get and TryGet always succeed, reusing a
// cached block or allocating one. A background refill keeps a few spare
// blocks ready for multi-block parts, and a cleaner halves the cache after
// each idle interval until it is empty.
//
// All methods are safe for concurrent use.
type defaultBufferPool struct {
	blockSize   int
	idleTimeout time.Duration

	// active is set by Get and Put and cleared by the cleaner on each tick.
	active atomic.Bool
	// refilling is set while a background refill goroutine is running.
	refilling atomic.Bool

	// mu guards the fields below.
	mu sync.Mutex
	// free is a LIFO stack of cached blocks, so the most recently used (and
	// most likely CPU-cache-warm) block is reused first.
	free           [][]byte
	cleanerRunning bool
	closed         bool
	// stop is closed by close to terminate the cleaner.
	stop chan struct{}
	// wg tracks the cleaner and refill goroutines.
	wg sync.WaitGroup
}

// newDefaultBufferPool returns an empty pool. It starts no goroutines.
func newDefaultBufferPool() *defaultBufferPool {
	return &defaultBufferPool{
		blockSize:   poolBlockSize,
		idleTimeout: poolIdleTimeout,
		stop:        make(chan struct{}),
	}
}

// Get returns a buffer of min(maxSize, blockSize) bytes, reusing a cached
// block or allocating one. It never blocks and only fails if maxSize is not
// positive.
func (p *defaultBufferPool) Get(maxSize int) ([]byte, error) {
	if maxSize <= 0 {
		return nil, errInvalidBufferSize
	}
	p.markActive()
	p.mu.Lock()
	var b []byte
	if n := len(p.free); n > 0 {
		b = p.free[n-1]
		p.free[n-1] = nil // Do not retain the block via the backing array.
		p.free = p.free[:n-1]
	}
	cached := len(p.free)
	p.mu.Unlock()
	if b == nil {
		b = make([]byte, p.blockSize)
	}
	p.maybeRefill(maxSize, cached)
	return b[:min(maxSize, p.blockSize)], nil
}

// TryGet is Get. The default pool has no memory limit, so it never refuses a
// buffer; returning false is reserved for custom pools that enforce one.
func (p *defaultBufferPool) TryGet(maxSize int) ([]byte, bool, error) {
	b, err := p.Get(maxSize)
	return b, err == nil, err
}

// Put returns a buffer obtained from Get or TryGet to the pool. Buffers whose
// capacity is not blockSize are dropped. Put must be called exactly once per
// buffer.
func (p *defaultBufferPool) Put(buf []byte) {
	if cap(buf) != p.blockSize {
		return
	}
	p.markActive()
	p.push(buf)
}

// markActive records activity for the cleaner. It skips the store when the
// flag is already set, to avoid contended writes on every call.
func (p *defaultBufferPool) markActive() {
	if !p.active.Load() {
		p.active.Store(true)
	}
}

// push caches b and starts the cleaner if it is not running. It returns false,
// dropping b, if the pool is closed.
func (p *defaultBufferPool) push(b []byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.free = append(p.free, b)
	if !p.cleanerRunning {
		p.cleanerRunning = true
		p.wg.Add(1)
		go p.runCleaner()
	}
	return true
}

// cached returns the number of cached blocks.
func (p *defaultBufferPool) cached() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.free)
}

// shrinkLocked drops cached blocks until keep remain. p.mu must be held.
func (p *defaultBufferPool) shrinkLocked(keep int) {
	clear(p.free[keep:])
	p.free = p.free[:keep]
}

// blocksFor returns the number of blocks needed to hold size > 0 bytes,
// computed without overflow for large sizes.
func (p *defaultBufferPool) blocksFor(size int) int {
	return 1 + (size-1)/p.blockSize
}

// maybeRefill pre-allocates, in the background, the spare blocks still needed
// for the rest of a maxSize part beyond the block just handed out, capped at
// poolMaxRefillBlocks. A refill allocates at most that many blocks, and only
// one refill runs at a time.
func (p *defaultBufferPool) maybeRefill(maxSize, cached int) {
	need := min(p.blocksFor(maxSize)-1, poolMaxRefillBlocks)
	if cached >= need {
		return
	}
	if !p.refilling.CompareAndSwap(false, true) {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.refilling.Store(false)
		return
	}
	p.wg.Add(1)
	p.mu.Unlock()
	go func() {
		defer p.wg.Done()
		defer p.refilling.Store(false)
		for i := cached; i < need; i++ {
			if !p.push(make([]byte, p.blockSize)) {
				return
			}
		}
	}()
}

// runCleaner calls shrinkIfIdle every idleTimeout until the cache is empty or
// the pool is closed.
func (p *defaultBufferPool) runCleaner() {
	defer p.wg.Done()
	t := time.NewTicker(p.idleTimeout)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			if p.shrinkIfIdle() {
				return
			}
		}
	}
}

// shrinkIfIdle evicts half of the cached blocks if there was no Get or Put
// since the previous call. It returns true, marking the cleaner stopped, once
// the cache is empty.
func (p *defaultBufferPool) shrinkIfIdle() (stopped bool) {
	if p.active.Swap(false) {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.shrinkLocked(len(p.free) / 2)
	if len(p.free) > 0 {
		return false
	}
	p.cleanerRunning = false
	return true
}

// close stops the background goroutines and releases cached blocks. Buffers
// Put afterwards are dropped.
func (p *defaultBufferPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.stop)
	p.mu.Unlock()
	p.wg.Wait()

	p.mu.Lock()
	p.shrinkLocked(0)
	p.cleanerRunning = false
	p.mu.Unlock()
}
