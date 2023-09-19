package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type TimestampStatisticsBuilder struct {
	// 继承
	LongValueStatisticsBuilder

	nonNullValueCount  int64
	minimum            int64
	maximum            int64
	bloomFilterBuilder BloomFilterBuilder

	// GetMillis(kind block.Type, block block.Block, position int32) int64
	// block.Type.GetLong
	millisFunction func(_ block.Type, block block.Block, position int32) int64
}

// type MillisFunction interface {
// 	GetMillis(kind block.Type, block block.Block, position int32) int64
// }

func NewTimestampStatisticsBuilder(bloomFilterBuilder BloomFilterBuilder) *TimestampStatisticsBuilder {
	return NewTimestampStatisticsBuilder3(bloomFilterBuilder, block.Type.GetLong)
}
func NewTimestampStatisticsBuilder2(millisFunction func(_ block.Type, block block.Block, position int32) int64) *TimestampStatisticsBuilder {
	return NewTimestampStatisticsBuilder3(NewNoOpBloomFilterBuilder(), millisFunction)
}
func NewTimestampStatisticsBuilder3(bloomFilterBuilder BloomFilterBuilder, millisFunction func(_ block.Type, block block.Block, position int32) int64) *TimestampStatisticsBuilder {
	tr := new(TimestampStatisticsBuilder)
	tr.bloomFilterBuilder = bloomFilterBuilder
	tr.millisFunction = millisFunction
	return tr
}

// @Override
func (tr *TimestampStatisticsBuilder) GetValueFromBlock(kind block.Type, block block.Block, position int32) int64 {
	return tr.millisFunction(kind, block, position)
}

// @Override
func (tr *TimestampStatisticsBuilder) AddValue(value int64) {
	tr.nonNullValueCount++
	tr.minimum = maths.Min(value, tr.minimum)
	tr.maximum = maths.Max(value, tr.maximum)
	tr.bloomFilterBuilder.AddLong(value)
}

func (tr *TimestampStatisticsBuilder) addTimestampStatistics(valueCount int64, value *TimestampStatistics) {
	tr.nonNullValueCount += valueCount
	tr.minimum = maths.Min(value.GetMin(), tr.minimum)
	tr.maximum = maths.Max(value.GetMax(), tr.maximum)
}

func (tr *TimestampStatisticsBuilder) buildTimestampStatistics() *optional.Optional[*TimestampStatistics] {
	if tr.nonNullValueCount == 0 {
		return optional.Empty[*TimestampStatistics]()
	}
	return optional.Of(NewTimestampStatistics(tr.minimum, tr.maximum))
}

// @Override
func (tr *TimestampStatisticsBuilder) BuildColumnStatistics() *ColumnStatistics {
	timestampStatistics := tr.buildTimestampStatistics()
	return NewColumnStatistics(tr.nonNullValueCount, optional.Map(timestampStatistics, func(s *TimestampStatistics) int64 {
		return TIMESTAMP_VALUE_BYTES
	}).OrElse(0), nil, nil, nil, nil, nil, timestampStatistics.OrElse(nil), nil, nil, tr.bloomFilterBuilder.BuildBloomFilter())
}

func MergeTimestampStatistics(stats *util.ArrayList[*ColumnStatistics]) *optional.Optional[*TimestampStatistics] {
	timestampStatisticsBuilder := NewTimestampStatisticsBuilder(NewNoOpBloomFilterBuilder())
	for i := 0; i < stats.Size(); i++ {
		columnStatistics := stats.Get(i)
		partialStatistics := columnStatistics.GetTimestampStatistics()
		if columnStatistics.GetNumberOfValues() > 0 {
			if partialStatistics == nil {
				return optional.Empty[*TimestampStatistics]()
			}
			timestampStatisticsBuilder.addTimestampStatistics(columnStatistics.GetNumberOfValues(), partialStatistics)
		}
	}
	return timestampStatisticsBuilder.buildTimestampStatistics()
}
