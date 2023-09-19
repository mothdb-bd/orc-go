package memory

import (
	"sync"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ChildAggregatedMemoryContext struct {
	// 继承
	AbstractAggregatedMemoryContext

	parentMemoryContext AggregatedMemoryContext
}

func NewChildAggregatedMemoryContext(parentMemoryContext AggregatedMemoryContext) *ChildAggregatedMemoryContext {
	ct := new(ChildAggregatedMemoryContext)
	ct.parentMemoryContext = parentMemoryContext
	ct.lock = new(sync.Mutex)
	return ct
}

// @Override
func (ct *ChildAggregatedMemoryContext) updateBytes(allocationTag string, delta int64) {
	ct.lock.Lock()

	if ct.isClosed() {
		panic("ChildAggregatedMemoryContext is already closed")
	}
	ct.parentMemoryContext.updateBytes(allocationTag, delta)
	ct.addBytes(delta)

	ct.lock.Unlock()
}

// @Override
func (ct *ChildAggregatedMemoryContext) tryUpdateBytes(allocationTag string, delta int64) bool {
	ct.lock.Lock()

	if ct.parentMemoryContext.tryUpdateBytes(allocationTag, delta) {
		ct.addBytes(delta)
		ct.lock.Unlock()
		return true
	} else {
		ct.lock.Unlock()
	}
	return false

}

// @Override
func (ct *ChildAggregatedMemoryContext) getParent() AggregatedMemoryContext {
	return ct.parentMemoryContext
}

// @Override
func (ct *ChildAggregatedMemoryContext) closeContext() {
	ct.parentMemoryContext.updateBytes(FORCE_FREE_TAG, -ct.GetBytes())
}

// @Override synchronized
func (at *ChildAggregatedMemoryContext) GetBytes() int64 {
	return at.usedBytes
}

// @Override synchronized
func (at *ChildAggregatedMemoryContext) Close() {
	at.lock.Lock()

	if at.closed {
		return
	}
	at.closed = true
	at.closeContext()
	at.usedBytes = 0

	at.lock.Unlock()
}

func (at *ChildAggregatedMemoryContext) isClosed() bool {
	return at.closed
}

func (at *ChildAggregatedMemoryContext) addBytes(bytes int64) {
	at.usedBytes = util.AddExactInt64(at.usedBytes, bytes)
}

// @Override
func (at *ChildAggregatedMemoryContext) NewAggregatedMemoryContext() AggregatedMemoryContext {
	ct := new(ChildAggregatedMemoryContext)
	ct.parentMemoryContext = at
	return ct
}

// @Override
func (at *ChildAggregatedMemoryContext) NewLocalMemoryContext(allocationTag string) LocalMemoryContext {
	return NewSimpleLocalMemoryContext(at, allocationTag)
}
