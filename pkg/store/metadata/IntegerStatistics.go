package metadata

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	INTEGER_VALUE_BYTES              int64 = util.BYTE_BYTES + util.INT64_BYTES
	INTEGER_STATISTICS_INSTANCE_SIZE int32 = util.SizeOf(&IntegerStatistics{})
)

type IntegerStatistics struct {
	// 继承
	RangeStatistics[int64]
	// 继承
	Hashable

	hasMinimum bool
	hasMaximum bool
	hasSum     bool
	minimum    int64
	maximum    int64
	sum        int64
}

func NewIntegerStatistics(minimum int64, maximum int64, sum int64) *IntegerStatistics {
	is := new(IntegerStatistics)
	is.hasMinimum = &minimum != nil
	is.minimum = util.Ternary(is.hasMinimum, minimum, 0)
	is.hasMaximum = &maximum != nil
	is.maximum = util.Ternary(is.hasMaximum, maximum, 0)
	is.hasSum = &sum != nil
	is.sum = util.Ternary(is.hasSum, sum, 0)
	return is
}

// @Override
func (is *IntegerStatistics) GetMin() int64 {
	if is.hasMinimum {
		return is.minimum
	} else {
		return math.MinInt64
	}
	// return util.Ternary(is.hasMinimum, is.minimum, basic.NullT[int64]())
}

// @Override
func (is *IntegerStatistics) GetMinPtr() *int64 {
	min := is.GetMin()
	return &min
}

// @Override
func (is *IntegerStatistics) GetMax() int64 {
	if is.hasMaximum {
		return is.maximum
	} else {
		return math.MaxInt64
	}
	// return util.Ternary(is.hasMaximum, is.maximum, basic.NullT[int64]())
}

// @Override
func (is *IntegerStatistics) GetMaxPtr() *int64 {
	max := is.GetMax()
	return &max
}

func (is *IntegerStatistics) GetSum() int64 {
	if is.hasSum {
		return is.sum
	} else {
		return math.MinInt64
	}
	// return util.Ternary(is.hasSum, is.sum, basic.NullT[int64]())
}

func (is *IntegerStatistics) GetSumPtr() *int64 {
	sum := is.GetSum()
	return &sum
}

// @Override
func (is *IntegerStatistics) GetRetainedSizeInBytes() int64 {
	return int64(INTEGER_STATISTICS_INSTANCE_SIZE)
}

// @Override
func (is *IntegerStatistics) String() string {
	return util.NewSB().AddInt64("min", is.GetMin()).AddInt64("max", is.GetMax()).AddInt64("sum", is.GetSum()).String()
}

// @Override
func (is *IntegerStatistics) AddHash(hasher *StatisticsHasher) {
	hasher.PutOptionalLong(is.hasMinimum, is.minimum).PutOptionalLong(is.hasMaximum, is.maximum).PutOptionalLong(is.hasSum, is.sum)
}
