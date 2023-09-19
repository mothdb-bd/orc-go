package metadata

import "github.com/mothdb-bd/orc-go/pkg/basic"

type RangeStatistics[T basic.Object] interface {
	GetMin() T
	GetMax() T
	GetRetainedSizeInBytes() int64
}
