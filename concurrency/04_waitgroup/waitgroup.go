package waitgroup

import (
	"sync/atomic"

	"primitives/internal/futex"
)

type WaitGroup struct {
	count   uint32
	waiters uint32
}

func (wg *WaitGroup) Add(delta int) {
	v := int32(atomic.AddUint32(&wg.count, uint32(delta)))
	if v < 0 {
		panic("negative WaitGroup counter")
	}
	if v == 0 && atomic.LoadUint32(&wg.waiters) != 0 {
		futex.WakeAll(&wg.count)
	}
}

func (wg *WaitGroup) Done() {
	wg.Add(-1)
}

func (wg *WaitGroup) Wait() {
	for {
		v := atomic.LoadUint32(&wg.count)
		if v == 0 {
			return
		}
		atomic.AddUint32(&wg.waiters, 1)
		futex.Wait(&wg.count, v)
		atomic.AddUint32(&wg.waiters, ^uint32(0))
	}
}
