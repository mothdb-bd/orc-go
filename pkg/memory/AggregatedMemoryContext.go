package memory

type AggregatedMemoryContext interface {
	GetBytes() int64

	Close()

	NewAggregatedMemoryContext() AggregatedMemoryContext

	NewLocalMemoryContext(allocationTag string) LocalMemoryContext

	updateBytes(allocationTag string, delta int64)

	tryUpdateBytes(allocationTag string, delta int64) bool
}

// static AggregatedMemoryContext newSimpleAggregatedMemoryContext()
// {
// 	return new SimpleAggregatedMemoryContext();
// }
func newSimpleAggregatedMemoryContext() AggregatedMemoryContext {
	return NewSimpleAggregatedMemoryContext()
}
