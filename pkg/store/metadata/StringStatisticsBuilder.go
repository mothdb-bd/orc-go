package metadata

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type StringStatisticsBuilder struct {

	// 继承
	SliceColumnStatisticsBuilder

	stringStatisticsLimitInBytes int32
	nonNullValueCount            int64
	minimum                      *slice.Slice
	maximum                      *slice.Slice
	sum                          int64
	bloomFilterBuilder           BloomFilterBuilder
}

func NewStringStatisticsBuilder(stringStatisticsLimitInBytes int32, bloomFilterBuilder BloomFilterBuilder) *StringStatisticsBuilder {
	return NewStringStatisticsBuilder2(stringStatisticsLimitInBytes, 0, nil, nil, 0, bloomFilterBuilder)
}
func NewStringStatisticsBuilder2(stringStatisticsLimitInBytes int32, nonNullValueCount int64, minimum *slice.Slice, maximum *slice.Slice, sum int64, bloomFilterBuilder BloomFilterBuilder) *StringStatisticsBuilder {
	sr := new(StringStatisticsBuilder)
	sr.stringStatisticsLimitInBytes = stringStatisticsLimitInBytes
	sr.nonNullValueCount = nonNullValueCount
	sr.minimum = minimum
	sr.maximum = maximum
	sr.sum = sum
	sr.bloomFilterBuilder = bloomFilterBuilder
	return sr
}

func (sr *StringStatisticsBuilder) GetNonNullValueCount() int64 {
	return sr.nonNullValueCount
}

// @Override
func (sr *StringStatisticsBuilder) AddValue(value *slice.Slice) {
	if sr.nonNullValueCount == 0 {
		sr.minimum = value
		sr.maximum = value
	} else if sr.minimum != nil && value.CompareTo(sr.minimum) <= 0 {
		sr.minimum = value
	} else if sr.maximum != nil && value.CompareTo(sr.maximum) >= 0 {
		sr.maximum = value
	}
	sr.bloomFilterBuilder.AddString(value)
	sr.nonNullValueCount++
	sr.sum = maths.AddExact(sr.sum, int64(value.Size()))
}

func (sr *StringStatisticsBuilder) addStringStatistics(valueCount int64, value *StringStatistics) {
	if sr.nonNullValueCount == 0 {
		sr.minimum = value.GetMin()
		sr.maximum = value.GetMax()
	} else {
		if sr.minimum != nil && (value.GetMin() == nil || sr.minimum.CompareTo(value.GetMin()) > 0) {
			sr.minimum = value.GetMin()
		}
		if sr.maximum != nil && (value.GetMax() == nil || sr.maximum.CompareTo(value.GetMax()) < 0) {
			sr.maximum = value.GetMax()
		}
	}
	sr.nonNullValueCount += valueCount
	sr.sum = maths.AddExact(sr.sum, value.GetSum())
}

func (sr *StringStatisticsBuilder) buildStringStatistics() *optional.Optional[*StringStatistics] {
	if sr.nonNullValueCount == 0 {
		return optional.Empty[*StringStatistics]()
	}
	sr.minimum = sr.dropStringMinMaxIfNecessary(sr.minimum)
	sr.maximum = sr.dropStringMinMaxIfNecessary(sr.maximum)
	if sr.minimum == nil && sr.maximum == nil {
		return optional.Empty[*StringStatistics]()
	}
	return optional.Of(NewStringStatistics(sr.minimum, sr.maximum, sr.sum))
}

// @Override
func (sr *StringStatisticsBuilder) BuildColumnStatistics() *ColumnStatistics {
	stringStatistics := sr.buildStringStatistics()
	stringStatistics.IfPresent(func(s *StringStatistics) {
		util.Verify(sr.nonNullValueCount > 0)
	})
	return NewColumnStatistics(sr.nonNullValueCount, optional.Map(stringStatistics, func(s *StringStatistics) int64 {
		return STRING_VALUE_BYTES_OVERHEAD + sr.sum/sr.nonNullValueCount
	}).OrElse(0), nil, nil, nil, stringStatistics.OrElse(nil), nil, nil, nil, nil, sr.bloomFilterBuilder.BuildBloomFilter())
}

func MergeStringStatistics(stats *util.ArrayList[*ColumnStatistics]) *optional.Optional[*StringStatistics] {
	stringStatisticsBuilder := NewStringStatisticsBuilder(math.MaxInt32, NewNoOpBloomFilterBuilder())
	for i := 0; i < stats.Size(); i++ {
		columnStatistics := stats.Get(i)
		partialStatistics := columnStatistics.GetStringStatistics()
		if columnStatistics.GetNumberOfValues() > 0 {
			if partialStatistics == nil || (partialStatistics.GetMin() == nil && partialStatistics.GetMax() == nil) {
				return optional.Empty[*StringStatistics]()
			}
			stringStatisticsBuilder.addStringStatistics(columnStatistics.GetNumberOfValues(), partialStatistics)
		}
	}
	return stringStatisticsBuilder.buildStringStatistics()
}

func (sr *StringStatisticsBuilder) dropStringMinMaxIfNecessary(minOrMax *slice.Slice) *slice.Slice {
	if minOrMax == nil || int32(minOrMax.Size()) > sr.stringStatisticsLimitInBytes {
		return nil
	}
	if minOrMax.IsCompact() {
		return minOrMax
	}
	return minOrMax.Copy()
}
