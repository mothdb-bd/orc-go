package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/util"
	"github.com/shopspring/decimal"
)

var LONG_DECIMAL_LONG_DECIMAL_VALUE_BYTES int64 = 16

type LongDecimalStatisticsBuilder struct {
	// 继承
	StatisticsBuilder

	nonNullValueCount int64
	minimum           *decimal.Decimal
	maximum           *decimal.Decimal
}

func NewLongDecimalStatisticsBuilder() *LongDecimalStatisticsBuilder {
	lr := new(LongDecimalStatisticsBuilder)
	lr.minimum = new(decimal.Decimal)
	lr.maximum = new(decimal.Decimal)
	return lr
}

// @Override
func (lr *LongDecimalStatisticsBuilder) AddBlock(kind block.Type, b block.Block) {
	scale := (kind.(*block.DecimalType)).GetScale()
	for position := util.INT32_ZERO; position < b.GetPositionCount(); position++ {
		if !b.IsNull(position) {
			value := kind.GetObject(b, position).(*block.Int128)
			d := decimal.NewFromBigInt(value.AsBigInt(), scale)
			lr.AddValue(&d)
		}
	}
}

func (lr *LongDecimalStatisticsBuilder) AddValue(value *decimal.Decimal) {
	lr.nonNullValueCount++
	if lr.minimum == nil {
		lr.minimum = value
		lr.maximum = value
	} else {
		if lr.minimum.Cmp(*value) < 0 {
			lr.minimum = value
		}
		if lr.maximum.Cmp(*value) > 0 {
			lr.maximum = value
		}
	}
}

func (lr *LongDecimalStatisticsBuilder) addDecimalStatistics(valueCount int64, value *DecimalStatistics) {
	lr.nonNullValueCount += valueCount
	if lr.minimum == nil {
		lr.minimum = value.GetMin()
		lr.maximum = value.GetMax()
	} else {
		if lr.minimum.Cmp(*value.GetMin()) < 0 {
			lr.minimum = value.GetMin()
		}
		if lr.maximum.Cmp(*value.GetMax()) > 0 {
			lr.maximum = value.GetMax()
		}
	}
}

func (lr *LongDecimalStatisticsBuilder) buildDecimalStatistics() *optional.Optional[*DecimalStatistics] {
	if lr.nonNullValueCount == 0 {
		return optional.Empty[*DecimalStatistics]()
	}
	return optional.Of(NewDecimalStatistics(lr.minimum, lr.maximum, LONG_DECIMAL_LONG_DECIMAL_VALUE_BYTES))
}

// @Override
func (lr *LongDecimalStatisticsBuilder) BuildColumnStatistics() *ColumnStatistics {
	decimalStatistics := lr.buildDecimalStatistics()
	return NewColumnStatistics(lr.nonNullValueCount, optional.Map(decimalStatistics, func(s *DecimalStatistics) int64 {
		return DECIMAL_VALUE_BYTES_OVERHEAD + LONG_DECIMAL_LONG_DECIMAL_VALUE_BYTES
	}).OrElse(0), nil, nil, nil, nil, nil, nil, decimalStatistics.OrElse(nil), nil, nil)
}

func MergeDecimalStatistics(stats *util.ArrayList[*ColumnStatistics]) *optional.Optional[*DecimalStatistics] {
	decimalStatisticsBuilder := NewLongDecimalStatisticsBuilder()
	for i := 0; i < stats.Size(); i++ {
		columnStatistics := stats.Get(i)
		partialStatistics := columnStatistics.GetDecimalStatistics()
		if columnStatistics.GetNumberOfValues() > 0 {
			if partialStatistics == nil {
				return optional.Empty[*DecimalStatistics]()
			}
			decimalStatisticsBuilder.addDecimalStatistics(columnStatistics.GetNumberOfValues(), partialStatistics)
		}
	}
	return decimalStatisticsBuilder.buildDecimalStatistics()
}
