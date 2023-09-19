package metadata

import "github.com/mothdb-bd/orc-go/pkg/util"

var COLUMN_STATISTICS_INSTANCE_SIZE int32 = util.SizeOf(&ColumnStatistics{})

type ColumnStatistics struct {
	// 继承
	Hashable

	hasNumberOfValues          bool
	numberOfValues             int64
	minAverageValueSizeInBytes int64
	booleanStatistics          *BooleanStatistics
	integerStatistics          *IntegerStatistics
	doubleStatistics           *DoubleStatistics
	stringStatistics           *StringStatistics
	dateStatistics             *DateStatistics
	timestampStatistics        *TimestampStatistics
	decimalStatistics          *DecimalStatistics
	binaryStatistics           *BinaryStatistics
	bloomFilter                *BloomFilter
}

func NewColumnStatistics(numberOfValues int64, minAverageValueSizeInBytes int64, booleanStatistics *BooleanStatistics, integerStatistics *IntegerStatistics, doubleStatistics *DoubleStatistics, stringStatistics *StringStatistics, dateStatistics *DateStatistics, timestampStatistics *TimestampStatistics, decimalStatistics *DecimalStatistics, binaryStatistics *BinaryStatistics, bloomFilter *BloomFilter) *ColumnStatistics {
	cs := new(ColumnStatistics)
	cs.hasNumberOfValues = &numberOfValues != nil
	cs.numberOfValues = util.Ternary(cs.hasNumberOfValues, numberOfValues, 0)
	cs.minAverageValueSizeInBytes = minAverageValueSizeInBytes
	cs.booleanStatistics = booleanStatistics
	cs.integerStatistics = integerStatistics
	cs.doubleStatistics = doubleStatistics
	cs.stringStatistics = stringStatistics
	cs.dateStatistics = dateStatistics
	cs.timestampStatistics = timestampStatistics
	cs.decimalStatistics = decimalStatistics
	cs.binaryStatistics = binaryStatistics
	cs.bloomFilter = bloomFilter
	return cs
}

func (cs *ColumnStatistics) HasNumberOfValues() bool {
	return cs.hasNumberOfValues
}

func (cs *ColumnStatistics) GetNumberOfValues() int64 {
	return util.Ternary(cs.hasNumberOfValues, cs.numberOfValues, 0)
}

func (cs *ColumnStatistics) GetNumberOfValuesPtr() *uint64 {
	u := util.Ternary(cs.hasNumberOfValues, uint64(cs.numberOfValues), 0)
	return &u
}

func (cs *ColumnStatistics) HasMinAverageValueSizeInBytes() bool {
	return cs.HasNumberOfValues() && cs.numberOfValues > 0
}

func (cs *ColumnStatistics) GetMinAverageValueSizeInBytes() int64 {
	return cs.minAverageValueSizeInBytes
}

func (cs *ColumnStatistics) GetBooleanStatistics() *BooleanStatistics {
	return cs.booleanStatistics
}

func (cs *ColumnStatistics) GetDateStatistics() *DateStatistics {
	return cs.dateStatistics
}

func (cs *ColumnStatistics) GetDoubleStatistics() *DoubleStatistics {
	return cs.doubleStatistics
}

func (cs *ColumnStatistics) GetIntegerStatistics() *IntegerStatistics {
	return cs.integerStatistics
}

func (cs *ColumnStatistics) GetStringStatistics() *StringStatistics {
	return cs.stringStatistics
}

func (cs *ColumnStatistics) GetDecimalStatistics() *DecimalStatistics {
	return cs.decimalStatistics
}

func (cs *ColumnStatistics) GetBinaryStatistics() *BinaryStatistics {
	return cs.binaryStatistics
}

func (cs *ColumnStatistics) GetTimestampStatistics() *TimestampStatistics {
	return cs.timestampStatistics
}

func (cs *ColumnStatistics) GetBloomFilter() *BloomFilter {
	return cs.bloomFilter
}

func (cs *ColumnStatistics) WithBloomFilter(bloomFilter *BloomFilter) *ColumnStatistics {
	return NewColumnStatistics(cs.GetNumberOfValues(), cs.minAverageValueSizeInBytes, cs.booleanStatistics, cs.integerStatistics, cs.doubleStatistics, cs.stringStatistics, cs.dateStatistics, cs.timestampStatistics, cs.decimalStatistics, cs.binaryStatistics, bloomFilter)
}

func (cs *ColumnStatistics) GetRetainedSizeInBytes() int64 {
	retainedSizeInBytes := int64(COLUMN_STATISTICS_INSTANCE_SIZE)
	if cs.booleanStatistics != nil {
		retainedSizeInBytes += cs.booleanStatistics.GetRetainedSizeInBytes()
	}
	if cs.integerStatistics != nil {
		retainedSizeInBytes += cs.integerStatistics.GetRetainedSizeInBytes()
	}
	if cs.doubleStatistics != nil {
		retainedSizeInBytes += cs.doubleStatistics.GetRetainedSizeInBytes()
	}
	if cs.stringStatistics != nil {
		retainedSizeInBytes += cs.stringStatistics.GetRetainedSizeInBytes()
	}
	if cs.dateStatistics != nil {
		retainedSizeInBytes += cs.dateStatistics.GetRetainedSizeInBytes()
	}
	if cs.timestampStatistics != nil {
		retainedSizeInBytes += cs.timestampStatistics.GetRetainedSizeInBytes()
	}
	if cs.decimalStatistics != nil {
		retainedSizeInBytes += cs.decimalStatistics.GetRetainedSizeInBytes()
	}
	if cs.binaryStatistics != nil {
		retainedSizeInBytes += cs.binaryStatistics.GetRetainedSizeInBytes()
	}
	if cs.bloomFilter != nil {
		retainedSizeInBytes += cs.bloomFilter.GetRetainedSizeInBytes()
	}
	return retainedSizeInBytes
}

// @Override
func (cs *ColumnStatistics) AddHash(hasher *StatisticsHasher) {
	hasher.PutOptionalLong(cs.hasNumberOfValues, cs.numberOfValues).PutOptionalHashable(cs.booleanStatistics).PutOptionalHashable(cs.integerStatistics).PutOptionalHashable(cs.doubleStatistics).PutOptionalHashable(cs.stringStatistics).PutOptionalHashable(cs.dateStatistics).PutOptionalHashable(cs.timestampStatistics).PutOptionalHashable(cs.decimalStatistics).PutOptionalHashable(cs.binaryStatistics).PutOptionalHashable(cs.bloomFilter)
}

func MergeColumnStatistics(stats *util.ArrayList[*ColumnStatistics]) *ColumnStatistics {

	// numberOfRows := stats.stream().mapToLong(ColumnStatistics.GetNumberOfValues).sum()
	var numberOfRows int64
	stats.ForEach(func(column *ColumnStatistics) {
		numberOfRows += column.GetNumberOfValues()
	})

	// minAverageValueBytes = stats.stream().mapToLong(func(s interface{}) {
	// 	s.getMinAverageValueSizeInBytes() * s.getNumberOfValues()
	// }).sum() / numberOfRows
	minAverageValueBytes := util.INT64_ZERO
	if numberOfRows > 0 {
		var tmpSum int64
		stats.ForEach(func(s *ColumnStatistics) {
			tmpSum += s.GetMinAverageValueSizeInBytes() * s.GetNumberOfValues()
		})
		minAverageValueBytes = tmpSum / numberOfRows
	}
	return NewColumnStatistics(numberOfRows, minAverageValueBytes, MergeBooleanStatistics(stats).OrElse(nil), MergeIntegerStatistics(stats).OrElse(nil), MergeDoubleStatistics(stats).OrElse(nil), MergeStringStatistics(stats).OrElse(nil), MergeDateStatistics(stats).OrElse(nil), MergeTimestampStatistics(stats).OrElse(nil), MergeDecimalStatistics(stats).OrElse(nil), MergeBinaryStatistics(stats).OrElse(nil), nil)
}
