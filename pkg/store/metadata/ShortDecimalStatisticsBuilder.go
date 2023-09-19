package metadata

import (
	"math/big"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/shopspring/decimal"
)

var SHORT_DECIMAL_VALUE_BYTES int64 = 8

type ShortDecimalStatisticsBuilder struct {
	// 继承
	LongValueStatisticsBuilder

	scale             int32
	nonNullValueCount int64
	minimum           int64
	maximum           int64
}

func NewShortDecimalStatisticsBuilder(scale int32) *ShortDecimalStatisticsBuilder {
	sr := new(ShortDecimalStatisticsBuilder)
	sr.scale = scale
	return sr
}

// @Override
func (sr *ShortDecimalStatisticsBuilder) AddValue(value int64) {
	sr.nonNullValueCount++
	sr.minimum = maths.Min(value, sr.minimum)
	sr.maximum = maths.Max(value, sr.maximum)
}

func (sr *ShortDecimalStatisticsBuilder) buildDecimalStatistics() *optional.Optional[*DecimalStatistics] {
	if sr.nonNullValueCount == 0 {
		return optional.Empty[*DecimalStatistics]()
	}

	min := decimal.NewFromBigInt(big.NewInt(sr.minimum), sr.scale)
	max := decimal.NewFromBigInt(big.NewInt(sr.maximum), sr.scale)
	return optional.Of(NewDecimalStatistics(&min, &max, SHORT_DECIMAL_VALUE_BYTES))
}

// @Override
func (sr *ShortDecimalStatisticsBuilder) BuildColumnStatistics() *ColumnStatistics {
	decimalStatistics := sr.buildDecimalStatistics()
	return NewColumnStatistics(sr.nonNullValueCount, optional.Map(decimalStatistics, func(s *DecimalStatistics) int64 {
		return DECIMAL_VALUE_BYTES_OVERHEAD + SHORT_DECIMAL_VALUE_BYTES
	}).OrElse(0), nil, nil, nil, nil, nil, nil, decimalStatistics.OrElse(nil), nil, nil)
}
