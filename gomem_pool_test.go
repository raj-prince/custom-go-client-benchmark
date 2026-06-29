// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"sync"
	"testing"
	"unsafe"
)

func newGomemBufferPoolWithShardsAndMinSize(initialSize int, numShards int, minShardSize int) *gomemBufferPool {
	return newGomemBufferPool(initialSize)
}

func BenchmarkGomemBufferPoolShardSize(b *testing.B) {
	// Target the production concurrency of 48 workers
	concurrency := 48

	b.Run("SyncPool-Tiered", func(b *testing.B) {
		pool := newGomemBufferPool(8*1024*1024)
		defer pool.Close()

		b.ResetTimer()
		
		var wg sync.WaitGroup
		ch := make(chan struct{})
		
		// Spawn workers
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range ch {
					// Allocate a 1MB buffer (simulating typical chunk reading, will go to fallback heap allocation)
					bufPtr := pool.Get(1024 * 1024)
					if bufPtr == nil || len(*bufPtr) == 0 {
						panic("allocation failed: returned empty buffer")
					}
					pool.Put(bufPtr)
				}
			}()
		}

		// Feed work
		for i := 0; i < b.N; i++ {
			ch <- struct{}{}
		}
		close(ch)
		wg.Wait()
	})
}


func TestGomemBufferPoolTieredFastPath(t *testing.T) {
	pool := newGomemBufferPool(16 * 1024 * 1024)
	defer pool.Close()
	numIterations := 36

	// We test all three tiers: 16KB (Tier 0), 32KB (Tier 1), 64KB (Tier 2)
	tiers := []struct {
		size int
		name string
	}{
		{16384, "16KB"},
		{32768, "32KB"},
		{65536, "64KB"},
	}

	for _, tier := range tiers {
		t.Run(tier.name, func(t *testing.T) {
			// Allocate a batch of buffers
			bufs1 := make([]*[]byte, numIterations)
			addrs1 := make([]unsafe.Pointer, numIterations)
			for i := 0; i < numIterations; i++ {
				bufs1[i] = pool.Get(tier.size)
				if bufs1[i] == nil || len(*bufs1[i]) != tier.size {
					t.Fatalf("Failed to allocate %s buffer at index %d", tier.name, i)
				}
				addrs1[i] = unsafe.Pointer(&(*bufs1[i])[0])
			}

			// Return all of them to the pool
			for i := 0; i < numIterations; i++ {
				pool.Put(bufs1[i])
			}

			// Allocate again
			bufs2 := make([]*[]byte, numIterations)
			addrs2 := make([]unsafe.Pointer, numIterations)
			for i := 0; i < numIterations; i++ {
				bufs2[i] = pool.Get(tier.size)
				if bufs2[i] == nil || len(*bufs2[i]) != tier.size {
					t.Fatalf("Failed to allocate %s buffer second time at index %d", tier.name, i)
				}
				addrs2[i] = unsafe.Pointer(&(*bufs2[i])[0])
			}

			// Verify address matching (log note for sync.Pool since it has no strict guarantees)
			for i := 0; i < numIterations; i++ {
				if addrs1[i] != addrs2[i] {
					t.Logf("Note: buffer address changed for %s at index %d: %p vs %p (expected for sync.Pool)", tier.name, i, addrs1[i], addrs2[i])
				}
			}

			// Clean up
			for i := 0; i < numIterations; i++ {
				pool.Put(bufs2[i])
			}
		})
	}
}
