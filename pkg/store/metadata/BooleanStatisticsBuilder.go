package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type BooleanStatisticsBuilder struct {
	// 继承
	StatisticsBuilder

	nonNullValueCount int64
	trueValueCount    int64
}

func NewBooleanStatisticsBuilder() *BooleanStatisticsBuilder {
	return new(BooleanStatisticsBuilder)
}

// @Override
func (br *BooleanStatisticsBuilder) AddBlock(kind block.Type, block block.Block) {
	for position := util.INT32_ZERO; position < block.GetPositionCount(); position++ {
		if !block.IsNull(position) {
			br.AddValue(kind.GetBoolean(block, position))
		}
	}
}

func (br *BooleanStatisticsBuilder) AddValue(value bool) {
	br.nonNullValueCount++
	if value {
		br.trueValueCount++
	}
}

func (br *BooleanStatisticsBuilder) addBooleanStatistics(valueCount int64, value *BooleanStatistics) {
	br.nonNullValueCount += valueCount
	br.trueValueCount += value.GetTrueValueCount()
}

func (br *BooleanStatisticsBuilder) buildBooleanStatistics() *optional.Optional[*BooleanStatistics] {
	if br.nonNullValueCount == 0 {
		return optional.Empty[*BooleanStatistics]()
	}
	return optional.Of(NewBooleanStatistics(br.trueValueCount))
}

// @Override
func (br *BooleanStatisticsBuilder) BuildColumnStatistics() *ColumnStatistics {
	booleanStatistics := br.buildBooleanStatistics()
	return NewColumnStatistics(br.nonNullValueCount, optional.Map(booleanStatistics, func(s *BooleanStatistics) int64 {
		return BOOLEAN_VALUE_BYTES
	}).OrElse(int64(0)), booleanStatistics.OrElse(nil), nil, nil, nil, nil, nil, nil, nil, nil)
}

func MergeBooleanStatistics(stats *util.ArrayList[*ColumnStatistics]) *optional.Optional[*BooleanStatistics] {
	booleanStatisticsBuilder := NewBooleanStatisticsBuilder()
	for i := 0; i < stats.Size(); i++ {
		columnStatistics := stats.Get(i)
		partialStatistics := columnStatistics.GetBooleanStatistics()
		if columnStatistics.GetNumberOfValues() > 0 {
			if partialStatistics == nil {
				return optional.Empty[*BooleanStatistics]()
			}
			booleanStatisticsBuilder.addBooleanStatistics(columnStatistics.GetNumberOfValues(), partialStatistics)
		}
	}
	return booleanStatisticsBuilder.buildBooleanStatistics()
}
