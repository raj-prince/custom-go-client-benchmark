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
	"fmt"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	goPageSize    = 4 * 1024 // 4KiB
	MADV_HUGEPAGE = 14
	MADV_DONTNEED = 4
)

// Link to the Go runtime's internal pinning functions.
//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin()

// bufferPool is a copy of the public bufferPool interface.
type bufferPool interface {
	Get(length int) *[]byte
	Put(*[]byte)
}

type sizedBufferPool struct {
	pool        sync.Pool
	defaultSize int
	shouldZero  bool
}

func (p *sizedBufferPool) Get(size int) *[]byte {
	buf, ok := p.pool.Get().(*[]byte)
	if !ok {
		buf := make([]byte, size, p.defaultSize)
		return &buf
	}
	b := *buf
	if p.shouldZero {
		clear(b[:cap(b)])
	}
	*buf = b[:size]
	return buf
}

func (p *sizedBufferPool) Put(buf *[]byte) {
	if cap(*buf) < p.defaultSize {
		return
	}
	p.pool.Put(buf)
}

type SimpleBufferPool struct {
	pool       sync.Pool
	shouldZero bool
}

func (p *SimpleBufferPool) Get(size int) *[]byte {
	bs, ok := p.pool.Get().(*[]byte)
	if ok && cap(*bs) >= size {
		if p.shouldZero {
			clear((*bs)[:cap(*bs)])
		}
		*bs = (*bs)[:size]
		return bs
	}

	if ok {
		p.pool.Put(bs)
	}

	allocSize := (size + goPageSize - 1) & ^(goPageSize - 1)
	b := make([]byte, size, allocSize)
	return &b
}

func (p *SimpleBufferPool) Put(buf *[]byte) {
	p.pool.Put(buf)
}

type NopBufferPool struct{}

func (NopBufferPool) Get(length int) *[]byte {
	b := make([]byte, length)
	return &b
}

func (NopBufferPool) Put(*[]byte) {}

// Arena represents a 2MB + 32KB mapped hugepage block containing 130 slots of 16KB.
type Arena struct {
	startAddr      uintptr
	endAddr        uintptr
	shardId        int
	arenaId        int
	memory         []byte
	allocatedSlots int32 // Atomic counter of active slot allocations
}

type gomemBufferPoolShard struct {
	id        int
	mu        sync.Mutex
	arenas    []*Arena
	freeSlots []uint32 // FIFO queue of packed slots
	head      int      // Head index of the FIFO queue
}

type mmap16KPool struct {
	shards []*gomemBufferPoolShard
}

// Global active Arenas list updated using Copy-On-Write (COW)
var globalArenas atomic.Pointer[[]*Arena]

func registerArena(newArena *Arena) {
	for {
		oldPtr := globalArenas.Load()
		var oldList []*Arena
		if oldPtr != nil {
			oldList = *oldPtr
		}

		newList := make([]*Arena, len(oldList)+1)
		copy(newList, oldList)
		newList[len(oldList)] = newArena

		// Sort by start address for fast binary search in Put
		sort.Slice(newList, func(i, j int) bool {
			return newList[i].startAddr < newList[j].startAddr
		})

		if globalArenas.CompareAndSwap(oldPtr, &newList) {
			break
		}
	}
}

func unregisterArena(arenaId int, shardId int) {
	for {
		oldPtr := globalArenas.Load()
		if oldPtr == nil {
			return
		}
		oldList := *oldPtr

		newList := make([]*Arena, 0, len(oldList)-1)
		for _, a := range oldList {
			if a != nil && !(a.shardId == shardId && a.arenaId == arenaId) {
				newList = append(newList, a)
			}
		}

		if globalArenas.CompareAndSwap(oldPtr, &newList) {
			break
		}
	}
}

func findArena(addr uintptr) *Arena {
	ptr := globalArenas.Load()
	if ptr == nil {
		return nil
	}
	list := *ptr

	// Binary search to find the first Arena where arena.endAddr > addr
	idx := sort.Search(len(list), func(i int) bool {
		return list[i].endAddr > addr
	})

	if idx < len(list) {
		arena := list[idx]
		if addr >= arena.startAddr && addr < arena.endAddr {
			return arena
		}
	}
	return nil
}

func newMmap16KPool() *mmap16KPool {
	numShards := 32
	shards := make([]*gomemBufferPoolShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &gomemBufferPoolShard{
			id:        i,
			arenas:    make([]*Arena, 0),
			freeSlots: make([]uint32, 0),
			head:      0,
		}
	}
	return &mmap16KPool{shards: shards}
}

func (p *mmap16KPool) Get(length int) *[]byte {
	cpu := procPin()
	shard := p.shards[cpu % 32]

	// Try to acquire the lock without blocking while pinned.
	// If it fails, unpin first to safely block, and then pin again.
	if !shard.mu.TryLock() {
		procUnpin()
		shard.mu.Lock()
		_ = procPin()
	}

	if len(shard.freeSlots)-shard.head == 0 {
		// Release lock and unpin to perform mmap safely without crashing the scheduler
		shard.mu.Unlock()
		procUnpin()

		var arenaSize int
		var numSlots int
		isFirstArena := (len(shard.arenas) == 0)
		if isFirstArena {
			arenaSize = 2*1024*1024 + 32*1024
			numSlots = 130
		} else {
			arenaSize = 1*1024*1024
			numSlots = 64
		}

		// Direct lock-free SYS_MMAP call!
		memory, err := rawMmap(arenaSize)
		if err != nil {
			panic(fmt.Sprintf("rawMmap failed for 16K pool: %v", err))
		}
		_ = madvise(memory, MADV_HUGEPAGE)

		startAddr := uintptr(unsafe.Pointer(&memory[0]))
		endAddr := startAddr + uintptr(arenaSize)

		newArena := &Arena{
			startAddr:      startAddr,
			endAddr:        endAddr,
			memory:         memory,
			allocatedSlots: 0,
		}

		registerArena(newArena)

		// Re-pin and re-lock safely: lock first while unpinned, then pin.
		cpu = procPin()
		procUnpin()
		shard = p.shards[cpu % 32]
		shard.mu.Lock()
		_ = procPin()

		arenaId := len(shard.arenas)
		newArena.shardId = shard.id
		newArena.arenaId = arenaId
		shard.arenas = append(shard.arenas, newArena)

		// Slice into slots
		for j := 0; j < numSlots; j++ {
			packed := (uint32(arenaId) << 16) | uint32(j)
			shard.freeSlots = append(shard.freeSlots, packed)
		}
	}

	packed := shard.freeSlots[shard.head]
	shard.head++

	// Amortized Compaction: reclaim slice capacity when head index grows
	if shard.head > 128 {
		shard.freeSlots = shard.freeSlots[shard.head:]
		shard.head = 0
	}

	arenaId := int(packed >> 16)
	slotIdx := int(packed & 0xFFFF)
	arena := shard.arenas[arenaId]

	// Increment allocatedSlots under the lock to prevent premature unmapping
	atomic.AddInt32(&arena.allocatedSlots, 1)

	shard.mu.Unlock()
	procUnpin()

	// Slice the slot (capacity is exactly 16KB)
	start := slotIdx * 16384
	buf := arena.memory[start : start+length : start+16384]

	bufPtr := new([]byte)
	*bufPtr = buf
	return bufPtr
}

func (p *mmap16KPool) Put(bufPtr *[]byte, arena *Arena, addr uintptr) {
	slotIndex := (addr - arena.startAddr) / 16384

	shard := p.shards[arena.shardId]
	shouldUnmap := false

	shard.mu.Lock()

	// Double-free protection: check if this slot is already free in the active queue.
	slotPacked := (uint32(arena.arenaId) << 16) | uint32(slotIndex)
	for i := shard.head; i < len(shard.freeSlots); i++ {
		if shard.freeSlots[i] == slotPacked {
			shard.mu.Unlock()
			*bufPtr = nil
			return // Ignore duplicate free
		}
	}

	active := atomic.AddInt32(&arena.allocatedSlots, -1)
	if active == 0 {
		cpu := getOSCPU()
		if cpu >= 0 && cpu < len(activeZeroCounters) {
			atomic.AddUint64(&activeZeroCounters[cpu], 1)
		}
	}

	// Dynamic Reclamation Check: unmap Arena if completely idle and not the only one
	if false && active == 0 && len(shard.arenas) > 1 {
		// Filter the active free slots to remove all slots belonging to this arenaId
		newFreeSlots := make([]uint32, 0, len(shard.freeSlots)-shard.head)
		for i := shard.head; i < len(shard.freeSlots); i++ {
			packed := shard.freeSlots[i]
			if int(packed>>16) != arena.arenaId {
				newFreeSlots = append(newFreeSlots, packed)
			}
		}
		shard.freeSlots = newFreeSlots
		shard.head = 0 // Reset head index on compaction/rebuild

		shard.arenas[arena.arenaId] = nil
		shouldUnmap = true

		unregisterArena(arena.arenaId, shard.id)
	} else {
		// FIFO: append to the end of the queue
		shard.freeSlots = append(shard.freeSlots, slotPacked)
	}
	shard.mu.Unlock()

	if shouldUnmap {
		// Direct lock-free SYS_MUNMAP call outside the lock!
		_ = rawMunmap(arena.memory)
	}
	*bufPtr = nil
}

type gomemBufferPool struct {
	exponentToNextLargestPoolMap     []int
	exponentToPreviousLargestPoolMap []int
	sizedPools                       []bufferPool
	fallbackPool                     bufferPool
	maxPoolCap                       int

	mmapPool *mmap16KPool
}

func newGomemBufferPool(initialSize int) *gomemBufferPool {
	exponents := []uint8{15, 16} // 32KB, 64KB (16KB is handled by custom mmapPool)

	uintSize := bits.UintSize
	maxExponent := uintSize - 2

	indexOfNextLargestBit := make([]int, maxExponent+1)
	indexOfPreviousLargestBit := make([]int, maxExponent+1)
	for i := range indexOfNextLargestBit {
		indexOfNextLargestBit[i] = -1
		indexOfPreviousLargestBit[i] = -1
	}

	maxTier := 0
	pools := make([]bufferPool, 0, len(exponents))

	for i, exp := range exponents {
		tierSize := 1 << exp
		pools = append(pools, &sizedBufferPool{
			defaultSize: tierSize,
			shouldZero:  true,
		})
		maxTier = max(maxTier, tierSize)

		indexOfNextLargestBit[exp] = i
		indexOfPreviousLargestBit[exp] = i
	}

	// Fill gaps for Get() (Next Largest)
	for i := maxExponent - 1; i >= 0; i-- {
		if indexOfNextLargestBit[i] == -1 {
			indexOfNextLargestBit[i] = indexOfNextLargestBit[i+1]
		}
	}

	// Fill gaps for Put() (Previous Largest)
	for i := 1; i <= maxExponent; i++ {
		if indexOfPreviousLargestBit[i] == -1 {
			indexOfPreviousLargestBit[i] = indexOfPreviousLargestBit[i-1]
		}
	}

	return &gomemBufferPool{
		exponentToNextLargestPoolMap:     indexOfNextLargestBit,
		exponentToPreviousLargestPoolMap: indexOfPreviousLargestBit,
		sizedPools:                       pools,
		maxPoolCap:                       maxTier,
		fallbackPool:                     &SimpleBufferPool{shouldZero: true},
		mmapPool:                         newMmap16KPool(),
	}
}

func (p *gomemBufferPool) Get(length int) *[]byte {
	if length > 0 && length <= 16384 {
		return p.mmapPool.Get(length)
	}
	return p.poolForGet(length).Get(length)
}

func (p *gomemBufferPool) poolForGet(size int) bufferPool {
	if size == 0 || size > p.maxPoolCap {
		return p.fallbackPool
	}
	querySize := uint(size - 1)
	poolIdx := p.exponentToNextLargestPoolMap[bits.Len(querySize)]
	if poolIdx == -1 {
		return p.fallbackPool
	}
	return p.sizedPools[poolIdx]
}

func (p *gomemBufferPool) Put(bufPtr *[]byte) {
	if bufPtr == nil {
		return
	}
	buf := *bufPtr
	if cap(buf) > 0 {
		addr := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
		if arena := findArena(addr); arena != nil {
			p.mmapPool.Put(bufPtr, arena, addr)
			return
		}
	}
	p.poolForPut(cap(*bufPtr)).Put(bufPtr)
}

func (p *gomemBufferPool) poolForPut(bCap int) bufferPool {
	if bCap == 0 {
		return NopBufferPool{}
	}
	if bCap > p.maxPoolCap {
		return p.fallbackPool
	}
	largestPowerOfTwo := bits.Len(uint(bCap)) - 1
	poolIdx := p.exponentToPreviousLargestPoolMap[largestPowerOfTwo]
	if poolIdx == -1 {
		return NopBufferPool{}
	}
	return p.sizedPools[poolIdx]
}

func (p *gomemBufferPool) Close() {
	if p.mmapPool != nil {
		for _, shard := range p.mmapPool.shards {
			shard.mu.Lock()
			for _, arena := range shard.arenas {
				if arena != nil && len(arena.memory) > 0 {
					_ = rawMunmap(arena.memory)
				}
			}
			shard.arenas = nil
			shard.freeSlots = nil
			shard.mu.Unlock()
		}
	}
}

func madvise(b []byte, advice int) error {
	if len(b) == 0 {
		return nil
	}
	_, _, errno := syscall.Syscall(
		syscall.SYS_MADVISE,
		uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)),
		uintptr(advice),
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// rawMmap makes a direct, lock-free SYS_MMAP system call (System Call 9) to the Linux kernel,
// completely bypassing Go's standard library global mmapper lock.
func rawMmap(length int) ([]byte, error) {
	prot := uintptr(syscall.PROT_READ | syscall.PROT_WRITE)
	flags := uintptr(syscall.MAP_ANON | syscall.MAP_PRIVATE)

	addr, _, errno := syscall.Syscall6(
		syscall.SYS_MMAP,
		0,
		uintptr(length),
		prot,
		flags,
		^uintptr(0), // -1 for fd
		0,           // 0 for offset
	)
	if errno != 0 {
		return nil, errno
	}

	// Convert raw uintptr to Go []byte slice via compiler-builtin unsafe.Slice
	return unsafe.Slice((*byte)(unsafe.Pointer(addr)), length), nil
}

// rawMunmap makes a direct, lock-free SYS_MUNMAP system call (System Call 11) to the Linux kernel,
// completely bypassing Go's standard library global mmapper lock.
func rawMunmap(memory []byte) error {
	addr := uintptr(unsafe.Pointer(&memory[0]))
	length := uintptr(cap(memory))

	_, _, errno := syscall.Syscall(
		syscall.SYS_MUNMAP,
		addr,
		length,
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

var activeZeroCounters [256]uint64 // Max 256 cores

// getOSCPU returns the OS CPU core ID currently executing the calling thread.
func getOSCPU() int {
	var cpu, node uint32
	// SYS_GETCPU is 309 on x86_64
	_, _, _ = syscall.RawSyscall(309, uintptr(unsafe.Pointer(&cpu)), uintptr(unsafe.Pointer(&node)), 0)
	return int(cpu)
}

func printActiveZeroCounters() {
	fmt.Println("--- Active 0 Hits Per CPU Core ---")
	var total uint64
	for cpu, count := range activeZeroCounters {
		if count > 0 {
			fmt.Printf("CPU %3d: %d hits\n", cpu, count)
			total += count
		}
	}
	fmt.Printf("Total Active 0 hits: %d\n", total)
}
