package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type LongTimestampType struct {
	// 继承
	TimestampType
}

func NewLongTimestampType(precision int32) *LongTimestampType {
	le := new(LongTimestampType)
	le.signature = NewTypeSignature(ST_TIMESTAMP, NumericParameter(int64(precision)))

	// NewlongTimestampType(precision, LongTimestamp.class)
	le.goKind = reflect.TypeOf(&LongTimestamp{}).Kind()
	le.precision = precision

	if precision < TIMESTAMP_MAX_SHORT_PRECISION+1 || precision > TIMESTAMP_MAX_PRECISION {
		panic(fmt.Sprintf("Precision must be in the range [%d, %d]", TIMESTAMP_MAX_SHORT_PRECISION+1, TIMESTAMP_MAX_PRECISION))
	}
	le.AbstractType = *NewAbstractType(le.signature, le.goKind)
	return le
}

// @Override
func (le *LongTimestampType) GetFixedSize() int32 {
	return util.INT64_BYTES + util.INT32_BYTES
}

// @Override
func (le *LongTimestampType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewInt96ArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/le.GetFixedSize()))
}

// @Override
func (le *LongTimestampType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return le.CreateBlockBuilder(blockBuilderStatus, expectedEntries, le.GetFixedSize())
}

// @Override
func (le *LongTimestampType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewInt96ArrayBlockBuilder(nil, positionCount)
}

// @Override
func (le *LongTimestampType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(getEpochMicros(block, position))
		blockBuilder.WriteInt(getFraction(block, position))
		blockBuilder.CloseEntry()
	}
}

// @Override
func (le *LongTimestampType) GetObject(block Block, position int32) basic.Object {
	return NewLongTimestamp(getEpochMicros(block, position), getFraction(block, position))
}

// @Override
func (le *LongTimestampType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	timestamp := value.(*LongTimestamp)
	le.Write(blockBuilder, timestamp.GetEpochMicros(), timestamp.GetPicosOfMicro())
}

func (le *LongTimestampType) Write(blockBuilder BlockBuilder, epochMicros int64, fraction int32) {
	blockBuilder.WriteLong(epochMicros)
	blockBuilder.WriteInt(fraction)
	blockBuilder.CloseEntry()
}

// @Override
func (te *LongTimestampType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}

func getEpochMicros(block Block, position int32) int64 {
	return block.GetLong(position, 0)
}

func getFraction(block Block, position int32) int32 {
	return block.GetInt(position, util.INT64_BYTES)
}
