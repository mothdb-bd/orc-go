package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type BinaryStatisticsBuilder struct {
	// 继承
	SliceColumnStatisticsBuilder

	nonNullValueCount int64
	sum               int64
}

func NewBinaryStatisticsBuilder() *BinaryStatisticsBuilder {
	nr := new(BinaryStatisticsBuilder)
	return nr
}

// @Override
func (br *BinaryStatisticsBuilder) AddValue(value *slice.Slice) {
	br.sum += value.LenInt64()
	br.nonNullValueCount++
}

func (br *BinaryStatisticsBuilder) buildBinaryStatistics() *optional.Optional[*BinaryStatistics] {
	if br.nonNullValueCount == 0 {
		return optional.Empty[*BinaryStatistics]()
	}
	return optional.Of(NewBinaryStatistics(br.sum))
}

func (br *BinaryStatisticsBuilder) addBinaryStatistics(valueCount int64, value *BinaryStatistics) {
	br.nonNullValueCount += valueCount
	br.sum += value.GetSum()
}

// @Override
func (br *BinaryStatisticsBuilder) BuildColumnStatistics() *ColumnStatistics {
	binaryStatistics := br.buildBinaryStatistics()
	binaryStatistics.IfPresent(func(s *BinaryStatistics) {
		util.Verify(br.nonNullValueCount > 0)
	})
	return NewColumnStatistics(br.nonNullValueCount, optional.Map(binaryStatistics, func(s *BinaryStatistics) int64 {
		return BINARY_VALUE_BYTES_OVERHEAD + br.sum/br.nonNullValueCount
	}).OrElse(int64(0)), nil, nil, nil, nil, nil, nil, nil, binaryStatistics.OrElse(nil), nil)
}

func MergeBinaryStatistics(stats *util.ArrayList[*ColumnStatistics]) *optional.Optional[*BinaryStatistics] {
	binaryStatisticsBuilder := NewBinaryStatisticsBuilder()
	for i := 0; i < stats.Size(); i++ {
		columnStatistics := stats.Get(i)
		partialStatistics := columnStatistics.GetBinaryStatistics()
		if columnStatistics.GetNumberOfValues() > 0 {
			if partialStatistics == nil {
				return optional.Empty[*BinaryStatistics]()
			}
			binaryStatisticsBuilder.addBinaryStatistics(columnStatistics.GetNumberOfValues(), partialStatistics)
		}
	}

	return binaryStatisticsBuilder.buildBinaryStatistics()
}
