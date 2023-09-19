package memory

type SimpleLocalMemoryContext struct {
	LocalMemoryContext

	parentMemoryContext AggregatedMemoryContext
	allocationTag       string //@GuardedBy("this")
	usedBytes           int64  //@GuardedBy("this")
	closed              bool
}

//@Override
func (st *SimpleLocalMemoryContext) GetBytes() int64 {
	return st.usedBytes
}

//@Override
func (st *SimpleLocalMemoryContext) SetBytes(bytes int64) {
	st.check(bytes)
	if bytes != st.usedBytes {
		st.parentMemoryContext.updateBytes(st.allocationTag, bytes-st.usedBytes)
		st.usedBytes = bytes
	}
}

//@Override
func (st *SimpleLocalMemoryContext) TrySetBytes(bytes int64) bool {
	st.check(bytes)
	if st.parentMemoryContext.tryUpdateBytes(st.allocationTag, bytes-st.usedBytes) {
		st.usedBytes = bytes
		return true
	}
	return false
}

func (st *SimpleLocalMemoryContext) check(bytes int64) {
	if st.closed {
		panic("SimpleLocalMemoryContext is already closed")
	}
	if bytes < 0 {
		panic("bytes cannot be negative")
	}
}

//@Override
func (st *SimpleLocalMemoryContext) Close() {
	if st.closed {
		return
	}
	st.closed = true
	st.parentMemoryContext.updateBytes(st.allocationTag, -st.usedBytes)
	st.usedBytes = 0
}

func NewSimpleLocalMemoryContext(parentMemoryContext AggregatedMemoryContext, allocationTag string) *SimpleLocalMemoryContext {
	st := new(SimpleLocalMemoryContext)
	st.parentMemoryContext = parentMemoryContext
	st.allocationTag = allocationTag
	return st
}
