package reliable

import (	
	"sync/atomic"
)

type AtomicBool struct {
	flag int32
}

func (b *AtomicBool) Set(value bool) {
	i := int32(0)
	if value {
		i = 1
	}
	atomic.StoreInt32(&b.flag, i)
}

func (b *AtomicBool) Get() bool {
	i := atomic.LoadInt32(&b.flag)
	return i != 0
}