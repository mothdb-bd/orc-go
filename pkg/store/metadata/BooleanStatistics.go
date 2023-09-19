package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type BooleanStatistics struct {
	// 继承
	Hashable

	trueValueCount int64
}

// var (
//
//	BooleanStatisticsBOOLEAN_VALUE_BYTES int64 = Byte.BYTES + Byte.BYTES
//	booleanStatisticsiNSTANCE_SIZE       int32 = ClassLayout.parseClass(BooleanStatistics.class).instanceSize()
//
// )
var (
	BOOLEAN_VALUE_BYTES   int64 = 1 + 1
	BOOLEAN_INSTANCE_SIZE int32 = util.SizeOf(&BooleanStatistics{})
)

func NewBooleanStatistics(trueValueCount int64) *BooleanStatistics {
	bs := new(BooleanStatistics)
	bs.trueValueCount = trueValueCount
	return bs
}

func (bs *BooleanStatistics) GetTrueValueCount() int64 {
	return bs.trueValueCount
}

func (bs *BooleanStatistics) GetRetainedSizeInBytes() int64 {
	return int64(BOOLEAN_INSTANCE_SIZE)
}

// @Override
func (bs *BooleanStatistics) Equals(o *BooleanStatistics) bool {
	return bs == o
}

// @Override
func (bs *BooleanStatistics) ToString() string {
	return util.NewSB().AddInt64("trueValueCount", bs.trueValueCount).ToStringHelper()
}
