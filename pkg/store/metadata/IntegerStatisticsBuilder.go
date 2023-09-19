package metadata

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type IntegerStatisticsBuilder struct {
	// 继承
	LongValueStatisticsBuilder

	nonNullValueCount  int64
	minimum            int64
	maximum            int64
	sum                int64
	overflow           bool
	bloomFilterBuilder BloomFilterBuilder
}

func NewIntegerStatisticsBuilder(bloomFilterBuilder BloomFilterBuilder) *IntegerStatisticsBuilder {
	ir := new(IntegerStatisticsBuilder)
	ir.bloomFilterBuilder = bloomFilterBuilder
	return ir
}

// @Override
func (ir *IntegerStatisticsBuilder) AddValue(value int64) {
	ir.nonNullValueCount++
	ir.minimum = maths.Min(value, ir.minimum)
	ir.maximum = maths.Max(value, ir.maximum)
	if !ir.overflow {
		ir.sum = maths.AddExact(ir.sum, value)
	}
	ir.bloomFilterBuilder.AddLong(value)
}

func (ir *IntegerStatisticsBuilder) addIntegerStatistics(valueCount int64, value *IntegerStatistics) {
	ir.nonNullValueCount += valueCount
	ir.minimum = maths.Min(value.GetMin(), ir.minimum)
	ir.maximum = maths.Max(value.GetMax(), ir.maximum)

	s := value.GetSum()
	if &s == nil {
		ir.overflow = true
	} else if !ir.overflow {
		ir.sum = maths.AddExact(ir.sum, value.GetSum())
	}
}

func (ir *IntegerStatisticsBuilder) buildIntegerStatistics() *optional.Optional[*IntegerStatistics] {
	if ir.nonNullValueCount == 0 {
		return optional.Empty[*IntegerStatistics]()
	}
	return optional.Of(NewIntegerStatistics(ir.minimum, ir.maximum, util.Ternary(ir.overflow, math.MinInt64, ir.sum)))
}

// @Override
func (ir *IntegerStatisticsBuilder) BuildColumnStatistics() *ColumnStatistics {
	integerStatistics := ir.buildIntegerStatistics()
	return NewColumnStatistics(ir.nonNullValueCount, optional.Map(integerStatistics, func(s *IntegerStatistics) int64 {
		return INTEGER_VALUE_BYTES
	}).OrElse(int64(0)), nil, integerStatistics.OrElse(nil), nil, nil, nil, nil, nil, nil, ir.bloomFilterBuilder.BuildBloomFilter())
}

func MergeIntegerStatistics(stats *util.ArrayList[*ColumnStatistics]) *optional.Optional[*IntegerStatistics] {
	integerStatisticsBuilder := NewIntegerStatisticsBuilder(NewNoOpBloomFilterBuilder())
	for i := 0; i < stats.Size(); i++ {
		columnStatistics := stats.Get(i)
		partialStatistics := columnStatistics.GetIntegerStatistics()
		if columnStatistics.GetNumberOfValues() > 0 {
			if partialStatistics == nil {
				return optional.Empty[*IntegerStatistics]()
			}
			integerStatisticsBuilder.addIntegerStatistics(columnStatistics.GetNumberOfValues(), partialStatistics)
		}
	}
	return integerStatisticsBuilder.buildIntegerStatistics()
}
