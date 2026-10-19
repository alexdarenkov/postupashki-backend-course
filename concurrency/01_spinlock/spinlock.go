package spinlock

import (
	"runtime"
	"sync/atomic"
)

const (
	maxBackoff = 64
	spinLimit  = 16
)

func spin(n int) {
	for range n {
	}
}

type Spinlock struct {
	locked atomic.Bool
}

func (s *Spinlock) Lock() {
	backoff := 1
	for !s.locked.CompareAndSwap(false, true) {
		if backoff <= maxBackoff {
			spin(backoff)
			backoff <<= 1
			continue
		}
		runtime.Gosched()
		backoff = 1
	}
}

func (s *Spinlock) TryLock() bool {
	return s.locked.CompareAndSwap(false, true)
}

func (s *Spinlock) Unlock() {
	if !s.locked.Swap(false) {
		panic("unlock of unlocked spinlock")
	}
}

type TTAS struct {
	locked atomic.Bool
}

func (s *TTAS) Lock() {
	for {
		for spins := 1; s.locked.Load(); spins++ {
			if spins%spinLimit == 0 {
				runtime.Gosched()
			}
		}
		if s.locked.CompareAndSwap(false, true) {
			return
		}
	}
}

func (s *TTAS) TryLock() bool {
	return !s.locked.Load() && s.locked.CompareAndSwap(false, true)
}

func (s *TTAS) Unlock() {
	if !s.locked.Swap(false) {
		panic("unlock of unlocked spinlock")
	}
}
