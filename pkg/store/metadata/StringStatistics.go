package metadata

import (
	"encoding/base64"
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	STRING_VALUE_BYTES_OVERHEAD     int64 = util.BYTE_BYTES + util.INT32_BYTES
	STRING_STATISTICS_INSTANCE_SIZE int32 = util.SizeOf(&StringStatistics{})
)

type StringStatistics struct { //@Nullable

	// 继承
	RangeStatistics[*slice.Slice]
	// 继承
	Hashable

	minimum *slice.Slice //@Nullable
	maximum *slice.Slice
	sum     int64
}

func NewStringStatistics(minimum *slice.Slice, maximum *slice.Slice, sum int64) *StringStatistics {
	ss := new(StringStatistics)
	if minimum != nil && maximum != nil && minimum.CompareTo(maximum) > 0 {
		panic(fmt.Sprintf("minimum is not less than or equal to maximum: '%s' [%s], '%s' [%s]", minimum.String(), base64.StdEncoding.EncodeToString(minimum.AvailableBytes()), maximum.String(), base64.StdEncoding.EncodeToString(maximum.AvailableBytes())))
	}
	ss.minimum = minimum
	ss.maximum = maximum
	ss.sum = sum
	return ss
}

// @Override
func (ss *StringStatistics) GetMin() *slice.Slice {
	return ss.minimum
}

// @Override
func (ss *StringStatistics) GetMax() *slice.Slice {
	return ss.maximum
}

func (ss *StringStatistics) GetSum() int64 {
	return ss.sum
}

func (ss *StringStatistics) GetSumPtr() *int64 {
	return &ss.sum
}

// @Override
func (ss *StringStatistics) GetRetainedSizeInBytes() int64 {
	return int64(STRING_STATISTICS_INSTANCE_SIZE + (util.Ternary(ss.minimum == nil, 0, ss.minimum.GetRetainedSize())) + (util.Ternary((ss.maximum == nil || ss.maximum == ss.minimum), 0, ss.maximum.GetRetainedSize())))
}

// @Override
func (ss *StringStatistics) ToString() string {
	return util.NewSB().AddString("min", util.Ternary(ss.minimum == nil, "<nil>", ss.minimum.String())).AddString("max", util.Ternary(ss.maximum == nil, "<nil>", ss.maximum.String())).AddInt64("sum", ss.sum).String()
}

// @Override
func (ss *StringStatistics) AddHash(hasher *StatisticsHasher) {
	hasher.PutOptionalSlice(ss.minimum).PutOptionalSlice(ss.maximum).PutLong(ss.sum)
}
