package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ShortTimestampWithTimeZoneType struct {
	// 继承
	TimestampWithTimeZoneType
}

func NewShortTimestampWithTimeZoneType(precision int32) *ShortTimestampWithTimeZoneType {
	se := new(ShortTimestampWithTimeZoneType)
	se.signature = NewTypeSignature(ST_TIMESTAMP_WITH_TIME_ZONE, NumericParameter(int64(precision)))
	se.goKind = reflect.Int64
	if precision < 0 || precision > TIMESTAMP_WITHTIMEZONE_MAX_PRECISION {
		panic(fmt.Sprintf("Precision must be in the range [0, %d]", TIMESTAMP_WITHTIMEZONE_MAX_PRECISION))
	}
	se.precision = precision

	se.AbstractType = *NewAbstractType(se.signature, se.goKind)
	return se
}

// @Override
func (se *ShortTimestampWithTimeZoneType) GetFixedSize() int32 {
	return util.INT64_BYTES
}

// @Override
func (se *ShortTimestampWithTimeZoneType) GetLong(block Block, position int32) int64 {
	return block.GetLong(position, 0)
}

// @Override
func (se *ShortTimestampWithTimeZoneType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, se.GetFixedSize())
}

// @Override
func (se *ShortTimestampWithTimeZoneType) WriteLong(blockBuilder BlockBuilder, value int64) {
	blockBuilder.WriteLong(value).CloseEntry()
}

// @Override
func (se *ShortTimestampWithTimeZoneType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(block.GetLong(position, 0)).CloseEntry()
	}
}

// @Override
func (se *ShortTimestampWithTimeZoneType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewLongArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/util.INT64_BYTES))
}

// @Override
func (se *ShortTimestampWithTimeZoneType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return se.CreateBlockBuilder(blockBuilderStatus, expectedEntries, util.INT64_BYTES)
}

// @Override
func (se *ShortTimestampWithTimeZoneType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewLongArrayBlockBuilder(nil, positionCount)
}

// @Override
func (te *ShortTimestampWithTimeZoneType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
