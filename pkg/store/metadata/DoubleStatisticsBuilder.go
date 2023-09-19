package metadata

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type DoubleStatisticsBuilder struct {
	// 继承
	StatisticsBuilder

	nonNullValueCount  int64
	hasNan             bool
	minimum            float64
	maximum            float64
	bloomFilterBuilder BloomFilterBuilder
}

func NewDoubleStatisticsBuilder(bloomFilterBuilder BloomFilterBuilder) *DoubleStatisticsBuilder {
	dr := new(DoubleStatisticsBuilder)
	dr.bloomFilterBuilder = bloomFilterBuilder
	return dr
}

// @Override
func (dr *DoubleStatisticsBuilder) AddBlock(kind block.Type, b block.Block) {
	for position := util.INT32_ZERO; position < b.GetPositionCount(); position++ {
		if !b.IsNull(position) {
			var value float64
			if kind.Equals(block.REAL) {
				value = float64(math.Float32frombits(uint32(kind.GetLong(b, position))))
			} else {
				value = kind.GetDouble(b, position)
			}
			dr.AddValue(value)
		}
	}
}

func (dr *DoubleStatisticsBuilder) AddValue(value float64) {
	dr.addValueInternal(value)
	dr.bloomFilterBuilder.AddDouble(value)
}

func (dr *DoubleStatisticsBuilder) AddValue2(value float32) {
	dr.addValueInternal(float64(value))
	dr.bloomFilterBuilder.AddFloat(value)
}

func (dr *DoubleStatisticsBuilder) addValueInternal(value float64) {
	dr.nonNullValueCount++
	if math.IsNaN(value) {
		dr.hasNan = true
	} else {
		dr.minimum = math.Min(value, dr.minimum)
		dr.maximum = math.Max(value, dr.maximum)
	}
}

func (dr *DoubleStatisticsBuilder) addDoubleStatistics(valueCount int64, value *DoubleStatistics) {
	dr.nonNullValueCount += valueCount
	dr.minimum = math.Min(value.GetMin(), dr.minimum)
	dr.maximum = math.Max(value.GetMax(), dr.maximum)
}

func (dr *DoubleStatisticsBuilder) buildDoubleStatistics() *optional.Optional[*DoubleStatistics] {
	if dr.nonNullValueCount == 0 || dr.hasNan {
		return optional.Empty[*DoubleStatistics]()
	}
	return optional.Of(NewDoubleStatistics(dr.minimum, dr.maximum))
}

// @Override
func (dr *DoubleStatisticsBuilder) BuildColumnStatistics() *ColumnStatistics {
	doubleStatistics := dr.buildDoubleStatistics()
	return NewColumnStatistics(dr.nonNullValueCount, optional.Map(doubleStatistics, func(s *DoubleStatistics) int64 {
		return DOUBLE_VALUE_BYTES
	}).OrElse(0), nil, nil, doubleStatistics.OrElse(nil), nil, nil, nil, nil, nil, dr.bloomFilterBuilder.BuildBloomFilter())
}

func MergeDoubleStatistics(stats *util.ArrayList[*ColumnStatistics]) *optional.Optional[*DoubleStatistics] {
	doubleStatisticsBuilder := NewDoubleStatisticsBuilder(NewNoOpBloomFilterBuilder())
	for i := 0; i < stats.Size(); i++ {
		columnStatistics := stats.Get(i)
		partialStatistics := columnStatistics.GetDoubleStatistics()
		if columnStatistics.GetNumberOfValues() > 0 {
			if partialStatistics == nil {
				return optional.Empty[*DoubleStatistics]()
			}
			doubleStatisticsBuilder.addDoubleStatistics(columnStatistics.GetNumberOfValues(), partialStatistics)
		}
	}
	return doubleStatisticsBuilder.buildDoubleStatistics()
}
