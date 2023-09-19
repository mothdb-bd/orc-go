package metadata

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	DATE_VALUE_BYTES              int64 = util.BYTE_BYTES + util.INT32_BYTES
	DATE_STATISTICS_INSTANCE_SIZE int32 = util.SizeOf(&DateStatistics{})
)

type DateStatistics struct {
	// 继承
	RangeStatistics[int32]
	// 继承
	Hashable

	hasMinimum bool
	hasMaximum bool
	minimum    int32
	maximum    int32
}

func NewDateStatistics(minimum int32, maximum int32) *DateStatistics {
	ds := new(DateStatistics)
	ds.hasMinimum = &minimum != nil
	ds.minimum = util.Ternary(ds.hasMinimum, minimum, 0)
	ds.hasMaximum = &maximum != nil
	ds.maximum = util.Ternary(ds.hasMaximum, maximum, 0)
	return ds
}

// @Override
func (ds *DateStatistics) GetMin() int32 {
	if ds.hasMinimum {
		return ds.minimum
	} else {
		return math.MinInt32
	}
	// return util.Ternary(ds.hasMinimum, ds.minimum, basic.NullT[int32]())
}

// @Override
func (ds *DateStatistics) GetMinPtr() *int32 {
	min := ds.GetMin()
	return &min
}

// @Override
func (ds *DateStatistics) GetMax() int32 {
	if ds.hasMaximum {
		return ds.maximum
	} else {
		return math.MaxInt32
	}
	// return util.Ternary(ds.hasMaximum, ds.maximum, basic.NullT[int32]())
}

// @Override
func (ds *DateStatistics) GetMaxPtr() *int32 {
	max := ds.GetMax()
	return &max
}

// @Override
func (ds *DateStatistics) GetRetainedSizeInBytes() int64 {
	return int64(DATE_STATISTICS_INSTANCE_SIZE)
}

// @Override
func (ds *DateStatistics) ToString() string {
	return util.NewSB().AddInt32("min", ds.GetMin()).AddInt32("max", ds.GetMax()).String()
}

// @Override
func (ds *DateStatistics) AddHash(hasher *StatisticsHasher) {
	hasher.PutOptionalInt(ds.hasMinimum, ds.minimum).PutOptionalInt(ds.hasMaximum, ds.maximum)
}
