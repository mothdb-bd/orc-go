package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ShortTimeWithTimeZoneType struct {
	// 继承
	TimeWithTimeZoneType
}

func NewShortTimeWithTimeZoneType(precision int32) *ShortTimeWithTimeZoneType {
	se := new(ShortTimeWithTimeZoneType)

	se.signature = NewTypeSignature(ST_TIME_WITH_TIME_ZONE, NumericParameter(int64(precision)))
	se.goKind = reflect.Int64
	se.precision = precision
	if precision < 0 || precision > TTZ_MAX_SHORT_PRECISION {
		panic(fmt.Sprintf("Precision must be in the range [0, %d]", TTZ_MAX_SHORT_PRECISION))
	}

	se.AbstractType = *NewAbstractType(se.signature, se.goKind)
	return se
}

// @Override
func (se *ShortTimeWithTimeZoneType) GetFixedSize() int32 {
	return util.INT64_BYTES
}

// @Override
func (se *ShortTimeWithTimeZoneType) GetLong(block Block, position int32) int64 {
	return block.GetLong(position, 0)
}

// @Override
func (se *ShortTimeWithTimeZoneType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, se.GetFixedSize())
}

// @Override
func (se *ShortTimeWithTimeZoneType) WriteLong(blockBuilder BlockBuilder, value int64) {
	blockBuilder.WriteLong(value).CloseEntry()
}

// @Override
func (se *ShortTimeWithTimeZoneType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(block.GetLong(position, 0)).CloseEntry()
	}
}

// @Override
func (se *ShortTimeWithTimeZoneType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewLongArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/util.INT64_BYTES))
}

// @Override
func (se *ShortTimeWithTimeZoneType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return se.CreateBlockBuilder(blockBuilderStatus, expectedEntries, util.INT64_BYTES)
}

// @Override
func (se *ShortTimeWithTimeZoneType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewLongArrayBlockBuilder(nil, positionCount)
}

// @Override
func (te *ShortTimeWithTimeZoneType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
