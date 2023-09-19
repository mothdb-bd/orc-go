package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/util"
	"github.com/shopspring/decimal"
)

var (
	DECIMAL_VALUE_BYTES_OVERHEAD         int64 = util.BYTE_BYTES
	DECIMAL_STATISTICS_INSTANCE_SIZE     int32 = util.SizeOf(&DecimalStatistics{})
	STATISTICS_BIG_DECIMAL_INSTANCE_SIZE int64 = int64(util.SizeOf(&decimal.Decimal{})) + int64(util.SizeOf(&block.BigInt{})) + util.SizeOfInt64(make([]int32, 0))
)

type DecimalStatistics struct {
	// 继承
	RangeStatistics[*decimal.Decimal]
	// 继承
	Hashable

	minimum             *decimal.Decimal
	maximum             *decimal.Decimal
	retainedSizeInBytes int64
}

func NewDecimalStatistics(minimum *decimal.Decimal, maximum *decimal.Decimal, decimalSizeInBytes int64) *DecimalStatistics {
	ds := new(DecimalStatistics)
	ds.minimum = minimum
	ds.maximum = maximum
	retainedSizeInBytes := util.INT64_ZERO
	if &minimum != nil {
		retainedSizeInBytes += STATISTICS_BIG_DECIMAL_INSTANCE_SIZE + decimalSizeInBytes
	}
	if &maximum != nil && minimum != maximum {
		retainedSizeInBytes += STATISTICS_BIG_DECIMAL_INSTANCE_SIZE + decimalSizeInBytes
	}
	ds.retainedSizeInBytes = retainedSizeInBytes + int64(DECIMAL_STATISTICS_INSTANCE_SIZE)
	return ds
}

// @Override
func (ds *DecimalStatistics) GetMin() *decimal.Decimal {
	return ds.minimum
}

// @Override
func (ds *DecimalStatistics) GetMinPtr() *string {
	min := ds.GetMin().String()
	return &min
}

// @Override
func (ds *DecimalStatistics) GetMax() *decimal.Decimal {
	return ds.maximum
}

// @Override
func (ds *DecimalStatistics) GetMaxPtr() *string {
	max := ds.GetMax().String()
	return &max
}

// @Override
func (ds *DecimalStatistics) GetRetainedSizeInBytes() int64 {
	return ds.retainedSizeInBytes
}

// @Override
func (ds *DecimalStatistics) String() string {
	return util.NewSB().AddString("minimum", ds.minimum.String()).AddString("maximum", ds.maximum.String()).String()
}

// @Override
func (ds *DecimalStatistics) AddHash(hasher *StatisticsHasher) {
	hasher.PutOptionalBigDecimal(ds.minimum).PutOptionalBigDecimal(ds.maximum)
}
