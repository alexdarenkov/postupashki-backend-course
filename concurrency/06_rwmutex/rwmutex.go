package rwmutex

import (
	"sync/atomic"

	"primitives/internal/futex"
)

const (
	writer        = 1 << 31
	writerWaiting = 1 << 30
	readersAsleep = 1 << 29
	readerMask    = readersAsleep - 1
)

type RWMutex struct {
	state uint32
	writerSeq uint32
	writersAsleep uint32
}

func (rw *RWMutex) RLock() {
	for {
		v := atomic.LoadUint32(&rw.state)
		if v&(writer|writerWaiting) == 0 {
			if atomic.CompareAndSwapUint32(&rw.state, v, v+1) {
				return
			}
			continue
		}
		if v&readersAsleep == 0 &&
			!atomic.CompareAndSwapUint32(&rw.state, v, v|readersAsleep) {
			continue
		}
		futex.Wait(&rw.state, v|readersAsleep)
	}
}

func (rw *RWMutex) RUnlock() {
	for {
		v := atomic.LoadUint32(&rw.state)
		if v&readerMask == 0 {
			panic("RUnlock of unlocked RWMutex")
		}
		if atomic.CompareAndSwapUint32(&rw.state, v, v-1) {
			if (v-1)&readerMask == 0 && v&writerWaiting != 0 {
				rw.wakeWriter()
			}
			return
		}
	}
}

func (rw *RWMutex) Lock() {
	for {
		v := atomic.LoadUint32(&rw.state)
		if v&(writer|readerMask) == 0 {
			if atomic.CompareAndSwapUint32(&rw.state, v, (v|writer)&^writerWaiting) {
				return
			}
			continue
		}
		if v&writerWaiting == 0 &&
			!atomic.CompareAndSwapUint32(&rw.state, v, v|writerWaiting) {
			continue
		}
		seq := atomic.LoadUint32(&rw.writerSeq)
		atomic.AddUint32(&rw.writersAsleep, 1)
		if atomic.LoadUint32(&rw.state)&(writer|readerMask) != 0 {
			futex.Wait(&rw.writerSeq, seq)
		}
		atomic.AddUint32(&rw.writersAsleep, ^uint32(0))
	}
}

func (rw *RWMutex) Unlock() {
	for {
		v := atomic.LoadUint32(&rw.state)
		if v&writer == 0 {
			panic("Unlock of unlocked RWMutex")
		}
		if atomic.CompareAndSwapUint32(&rw.state, v, v&^(writer|readersAsleep)) {
			if v&readersAsleep != 0 {
				futex.WakeAll(&rw.state)
			}
			rw.wakeWriter()
			return
		}
	}
}

func (rw *RWMutex) wakeWriter() {
	atomic.AddUint32(&rw.writerSeq, 1)
	if atomic.LoadUint32(&rw.writersAsleep) != 0 {
		futex.Wake(&rw.writerSeq)
	}
}
