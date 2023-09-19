package metadata

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	TIMESTAMP_VALUE_BYTES    int64 = util.BYTE_BYTES + util.INT64_BYTES
	STATISTICS_INSTANCE_SIZE int32 = util.SizeOf(&TimestampStatistics{})
)

type TimestampStatistics struct {
	// 继承
	RangeStatistics[int64]
	// 继承
	Hashable

	hasMinimum bool
	hasMaximum bool
	minimum    int64
	maximum    int64
}

func NewTimestampStatistics(minimum int64, maximum int64) *TimestampStatistics {
	ts := new(TimestampStatistics)
	ts.hasMinimum = &minimum != nil
	ts.minimum = util.Ternary(ts.hasMinimum, minimum, 0)
	ts.hasMaximum = &maximum != nil
	ts.maximum = util.Ternary(ts.hasMaximum, maximum, 0)
	return ts
}

// @Override
func (ts *TimestampStatistics) GetMin() int64 {
	return util.Ternary(ts.hasMinimum, ts.minimum, math.MinInt64)
}

// @Override
func (ts *TimestampStatistics) GetMinPtr() *int64 {
	min := ts.GetMin()
	return &min
}

// @Override
func (ts *TimestampStatistics) GetMax() int64 {
	return util.Ternary(ts.hasMaximum, ts.maximum, math.MaxInt64)
}

// @Override
func (ts *TimestampStatistics) GetMaxPtr() *int64 {
	max := ts.GetMax()
	return &max
}

// @Override
func (ts *TimestampStatistics) GetRetainedSizeInBytes() int64 {
	return int64(STATISTICS_INSTANCE_SIZE)
}

// @Override
func (ts *TimestampStatistics) ToString() string {
	return util.NewSB().AddInt64("min", ts.GetMin()).AddInt64("max", ts.GetMax()).String()
}

// @Override
func (ts *TimestampStatistics) AddHash(hasher *StatisticsHasher) {
	hasher.PutOptionalLong(ts.hasMinimum, ts.minimum).PutOptionalLong(ts.hasMaximum, ts.maximum)
}
