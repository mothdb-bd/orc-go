package memory

import (
	"sync"
)

var (
	FORCE_FREE_TAG string = "FORCE_FREE_OPERATION"
)

type AbstractAggregatedMemoryContext struct { //@GuardedBy("this")
	// 继承
	AggregatedMemoryContext

	usedBytes int64 //@GuardedBy("this")
	closed    bool

	lock *sync.Mutex
}
