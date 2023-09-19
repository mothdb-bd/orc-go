package metadata

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	DOUBLE_VALUE_BYTES              int64 = util.BYTE_BYTES + util.FLOAT64_BYTES
	DOUBLE_STATISTICS_INSTANCE_SIZE int32 = util.SizeOf(&DoubleStatistics{})
)

type DoubleStatistics struct {
	hasMinimum bool
	hasMaximum bool
	minimum    float64
	maximum    float64
}

func NewDoubleStatistics(minimum float64, maximum float64) *DoubleStatistics {
	ds := new(DoubleStatistics)
	ds.hasMinimum = &minimum != nil
	ds.minimum = util.Ternary(ds.hasMinimum, minimum, 0)
	ds.hasMaximum = &maximum != nil
	ds.maximum = util.Ternary(ds.hasMaximum, maximum, 0)
	return ds
}

// @Override
func (ds *DoubleStatistics) GetMin() float64 {
	if ds.hasMinimum {
		return ds.minimum
	} else {
		return float64(math.MinInt64)
	}
	// return util.Ternary(ds.hasMinimum, ds.minimum, basic.NullT[float64]())
}

// @Override
func (ds *DoubleStatistics) GetMinPtr() *float64 {
	min := ds.GetMin()
	return &min
}

// @Override
func (ds *DoubleStatistics) GetMax() float64 {
	if ds.hasMaximum {
		return ds.maximum
	} else {
		return float64(math.MaxInt64)
	}
	// return util.Ternary(ds.hasMaximum, ds.maximum, basic.NullT[float64]())
}

// @Override
func (ds *DoubleStatistics) GetMaxPtr() *float64 {
	max := ds.GetMax()
	return &max
}

// @Override
func (ds *DoubleStatistics) GetRetainedSizeInBytes() int64 {
	return int64(DOUBLE_STATISTICS_INSTANCE_SIZE)
}

// @Override
func (ds *DoubleStatistics) ToString() string {
	return util.NewSB().AddFloat64("min", ds.GetMin()).AddFloat64("max", ds.GetMax()).String()
}

// @Override
func (ds *DoubleStatistics) AddHash(hasher *StatisticsHasher) {
	hasher.PutOptionalDouble(ds.hasMinimum, ds.minimum).PutOptionalDouble(ds.hasMaximum, ds.maximum)
}
