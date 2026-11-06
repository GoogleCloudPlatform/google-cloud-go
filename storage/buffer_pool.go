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
	"time"
)

const (
	// poolBlockSize is the size of every block vended by the default buffer
	// pool. GCS parallel upload parts are multiples of 32 MiB, so each
	// minimum-sized part is backed by a single block.
	poolBlockSize = 32 << 20

	// poolCleanInterval is how often the cleaner runs. Each run evicts half of
	// the cached blocks that were not used since the previous run.
	poolCleanInterval = 30 * time.Second
)

var errInvalidBufferSize = errors.New("storage: buffer pool maxSize must be positive")

// defaultBufferPool is the BufferPool used for parallel uploads when the
// application does not provide one through experimental.WithBufferPool.
//
// It vends uniform 32 MiB blocks shared across all writers of a client. The
// pool has no memory limit; memory in use is bounded by the writers'
// PartSize and Concurrency settings. Get and TryGet always succeed, reusing a
// cached block or allocating one. A cleaner shrinks the cache toward the
// blocks actually in use, halving the unused ones every cleanInterval.
//
// All methods are safe for concurrent use.
type defaultBufferPool struct {
	blockSize     int
	cleanInterval time.Duration

	// mu guards the fields below.
	mu sync.Mutex
	// free is a LIFO stack of cached blocks. Get and Put only touch the top,
	// so the bottom minFree blocks are the ones left unused for a whole
	// cleaner interval.
	free [][]byte
	// minFree is the smallest len(free) since the last cleaner run. The
	// bottom minFree blocks were not used during that interval.
	minFree        int
	cleanerRunning bool
	closed         bool
	// stop is closed by close to terminate the cleaner.
	stop chan struct{}
	// wg tracks the cleaner goroutine.
	wg sync.WaitGroup
}

// newDefaultBufferPool returns an empty pool. It starts no goroutines.
func newDefaultBufferPool() *defaultBufferPool {
	return &defaultBufferPool{
		blockSize:     poolBlockSize,
		cleanInterval: poolCleanInterval,
		stop:          make(chan struct{}),
	}
}

// Get returns a buffer of min(maxSize, blockSize) bytes, reusing a cached
// block or allocating one.
func (p *defaultBufferPool) Get(maxSize int) ([]byte, error) {
	if maxSize <= 0 {
		return nil, errInvalidBufferSize
	}
	p.mu.Lock()
	var b []byte
	if n := len(p.free); n > 0 {
		b = p.free[n-1]
		p.free[n-1] = nil // Do not retain the block via the backing array.
		p.free = p.free[:n-1]
		p.minFree = min(p.minFree, len(p.free))
	}
	p.mu.Unlock()
	if b == nil {
		b = make([]byte, p.blockSize)
	}
	return b[:min(maxSize, p.blockSize)], nil
}

// TryGet behaves exactly like Get and never returns false. It exists because
// BufferPool requires it: custom pools that enforce a memory limit return
// false to make the uploader run fewer segments in parallel, but the default
// pool has no limit, so it never refuses a buffer.
func (p *defaultBufferPool) TryGet(maxSize int) ([]byte, bool, error) {
	b, err := p.Get(maxSize)
	return b, err == nil, err
}

// Put returns a buffer obtained from Get or TryGet to the pool and starts the
// cleaner if it is not running. Buffers whose capacity is not blockSize, and
// buffers returned after close, are dropped.
//
// Put must be called exactly once per buffer: putting a buffer twice caches it
// twice, so two later Get callers would share, and corrupt, the same memory.
func (p *defaultBufferPool) Put(buf []byte) {
	if cap(buf) != p.blockSize {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	// Cache the block at full length; Get re-slices it to the caller's size.
	p.free = append(p.free, buf[:p.blockSize])
	if !p.cleanerRunning {
		p.cleanerRunning = true
		p.minFree = len(p.free)
		p.wg.Add(1)
		go p.runCleaner()
	}
}

// shrinkLocked drops cached blocks until keep remain. p.mu must be held. It
// drops from the top of the stack, which is fine because blocks are
// interchangeable; only the count of unused blocks matters.
func (p *defaultBufferPool) shrinkLocked(keep int) {
	clear(p.free[keep:])
	p.free = p.free[:keep]
}

// runCleaner calls shrinkUnused every cleanInterval until the cache is empty or
// the pool is closed.
func (p *defaultBufferPool) runCleaner() {
	defer p.wg.Done()
	t := time.NewTicker(p.cleanInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			if p.shrinkUnused() {
				return
			}
		}
	}
}

// shrinkUnused evicts half (rounded up) of the blocks that stayed cached for
// the whole interval since the previous call. It returns true, marking the
// cleaner stopped, once the cache is empty.
func (p *defaultBufferPool) shrinkUnused() (stopped bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	evict := (p.minFree + 1) / 2
	p.shrinkLocked(len(p.free) - evict)
	p.minFree = len(p.free)
	if len(p.free) > 0 {
		return false
	}
	p.cleanerRunning = false
	return true
}

// close stops the cleaner and releases cached blocks. Buffers Put afterwards
// are dropped.
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
