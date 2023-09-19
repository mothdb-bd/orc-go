package memory

import (
	"sync"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

type SimpleAggregatedMemoryContext struct {
	// 继承
	AbstractAggregatedMemoryContext
}

func NewSimpleAggregatedMemoryContext() *SimpleAggregatedMemoryContext {
	st := new(SimpleAggregatedMemoryContext)
	st.lock = new(sync.Mutex)
	return st
}

// @Override
func (st *SimpleAggregatedMemoryContext) updateBytes(allocationTag string, delta int64) {
	if st.isClosed() {
		panic("SimpleAggregatedMemoryContext is already closed")
	}
	st.addBytes(delta)
}

// @Override
func (st *SimpleAggregatedMemoryContext) tryUpdateBytes(allocationTag string, delta int64) bool {
	st.addBytes(delta)
	return true
}

// @Override
func (st *SimpleAggregatedMemoryContext) getParent() *AbstractAggregatedMemoryContext {
	return nil
}

// @Override
func (st *SimpleAggregatedMemoryContext) closeContext() {
}

// @Override synchronized
func (at *SimpleAggregatedMemoryContext) GetBytes() int64 {
	return at.usedBytes
}

// @Override synchronized
func (at *SimpleAggregatedMemoryContext) Close() {
	at.lock.Lock()

	if at.closed {
		return
	}
	at.closed = true
	at.closeContext()
	at.usedBytes = 0

	at.lock.Unlock()
}

func (at *SimpleAggregatedMemoryContext) isClosed() bool {
	return at.closed
}

func (at *SimpleAggregatedMemoryContext) addBytes(bytes int64) {
	at.usedBytes = util.AddExactInt64(at.usedBytes, bytes)
}

// @Override
func (at *SimpleAggregatedMemoryContext) NewAggregatedMemoryContext() AggregatedMemoryContext {
	ct := new(SimpleAggregatedMemoryContext)
	ct.lock = new(sync.Mutex)
	return ct
}

// @Override
func (at *SimpleAggregatedMemoryContext) NewLocalMemoryContext(allocationTag string) LocalMemoryContext {
	return NewSimpleLocalMemoryContext(at, allocationTag)
}
