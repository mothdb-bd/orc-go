package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type DateStatisticsBuilder struct {
	// 继承
	LongValueStatisticsBuilder

	nonNullValueCount  int64
	minimum            int32
	maximum            int32
	bloomFilterBuilder BloomFilterBuilder
}

func NewDateStatisticsBuilder(bloomFilterBuilder BloomFilterBuilder) *DateStatisticsBuilder {
	dr := new(DateStatisticsBuilder)
	dr.bloomFilterBuilder = bloomFilterBuilder
	return dr
}

// @Override
func (dr *DateStatisticsBuilder) AddValue(value int64) {
	dr.nonNullValueCount++
	intValue, _ := util.ToInt32Exact(value)
	dr.minimum = maths.MinInt32(intValue, dr.minimum)
	dr.maximum = maths.MaxInt32(intValue, dr.maximum)
	dr.bloomFilterBuilder.AddLong(value)
}

func (dr *DateStatisticsBuilder) addDateStatistics(valueCount int64, value *DateStatistics) {
	dr.nonNullValueCount += valueCount
	dr.minimum = maths.MinInt32(value.GetMin(), dr.minimum)
	dr.maximum = maths.MaxInt32(value.GetMax(), dr.maximum)
}

func (dr *DateStatisticsBuilder) buildDateStatistics() *optional.Optional[*DateStatistics] {
	if dr.nonNullValueCount == 0 {
		return optional.Empty[*DateStatistics]()
	}
	return optional.Of(NewDateStatistics(dr.minimum, dr.maximum))
}

// @Override
func (dr *DateStatisticsBuilder) BuildColumnStatistics() *ColumnStatistics {
	dateStatistics := dr.buildDateStatistics()
	return NewColumnStatistics(dr.nonNullValueCount, optional.Map(dateStatistics, func(s *DateStatistics) int64 {
		return DATE_VALUE_BYTES
	}).OrElse(0), nil, nil, nil, nil, dateStatistics.OrElse(nil), nil, nil, nil, dr.bloomFilterBuilder.BuildBloomFilter())
}

func MergeDateStatistics(stats *util.ArrayList[*ColumnStatistics]) *optional.Optional[*DateStatistics] {
	dateStatisticsBuilder := NewDateStatisticsBuilder(NewNoOpBloomFilterBuilder())
	for i := 0; i < stats.Size(); i++ {
		columnStatistics := stats.Get(i)
		partialStatistics := columnStatistics.GetDateStatistics()
		if columnStatistics.GetNumberOfValues() > 0 {
			if partialStatistics == nil {
				return optional.Empty[*DateStatistics]()
			}
			dateStatisticsBuilder.addDateStatistics(columnStatistics.GetNumberOfValues(), partialStatistics)
		}
	}
	return dateStatisticsBuilder.buildDateStatistics()
}
