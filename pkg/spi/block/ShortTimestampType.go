package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ShortTimestampType struct {
	// 继承
	TimestampType
}

func NewShortTimestampType(precision int32) *ShortTimestampType {
	se := new(ShortTimestampType)
	se.signature = NewTypeSignature(ST_TIMESTAMP, NumericParameter(int64(precision)))
	se.goKind = reflect.Int64
	se.precision = precision
	if precision < 0 || precision > TIMESTAMP_MAX_SHORT_PRECISION {
		panic(fmt.Sprintf("Precision must be in the range [0, %d]", TIMESTAMP_MAX_SHORT_PRECISION))
	}
	se.AbstractType = *NewAbstractType(se.signature, se.goKind)
	return se
}

// @Override
func (se *ShortTimestampType) GetFixedSize() int32 {
	return util.INT64_BYTES
}

// @Override
func (se *ShortTimestampType) GetLong(block Block, position int32) int64 {
	return block.GetLong(position, 0)
}

// @Override
func (se *ShortTimestampType) WriteLong(blockBuilder BlockBuilder, value int64) {
	blockBuilder.WriteLong(value).CloseEntry()
}

// @Override
func (se *ShortTimestampType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(se.GetLong(block, position)).CloseEntry()
	}
}

// @Override
func (se *ShortTimestampType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewLongArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/util.INT64_BYTES))
}

// @Override
func (se *ShortTimestampType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return se.CreateBlockBuilder(blockBuilderStatus, expectedEntries, util.INT64_BYTES)
}

// @Override
func (se *ShortTimestampType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewLongArrayBlockBuilder(nil, positionCount)
}

// @Override
func (te *ShortTimestampType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
