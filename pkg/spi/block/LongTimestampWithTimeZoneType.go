package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type LongTimestampWithTimeZoneType struct {
	// 继承
	TimestampWithTimeZoneType
}

func NewLongTimestampWithTimeZoneType(precision int32) *LongTimestampWithTimeZoneType {
	le := new(LongTimestampWithTimeZoneType)

	le.signature = NewTypeSignature(ST_TIMESTAMP_WITH_TIME_ZONE, NumericParameter(int64(precision)))
	le.goKind = reflect.TypeOf(&LongTimestampWithTimeZone{}).Kind()
	le.precision = precision
	if precision < TIMESTAMP_WITHTIMEZONE_MAX_SHORT_PRECISION+1 || precision > TIMESTAMP_WITHTIMEZONE_MAX_PRECISION {
		panic(fmt.Sprintf("Precision must be in the range [%d, %d]", TIMESTAMP_WITHTIMEZONE_MAX_SHORT_PRECISION+1, TIMESTAMP_WITHTIMEZONE_MAX_PRECISION))
	}

	le.AbstractType = *NewAbstractType(le.signature, le.goKind)
	return le
}

// @Override
func (le *LongTimestampWithTimeZoneType) GetFixedSize() int32 {
	return util.INT64_BYTES + util.INT32_BYTES
}

// @Override
func (le *LongTimestampWithTimeZoneType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewInt96ArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/le.GetFixedSize()))
}

// @Override
func (le *LongTimestampWithTimeZoneType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return le.CreateBlockBuilder(blockBuilderStatus, expectedEntries, le.GetFixedSize())
}

// @Override
func (le *LongTimestampWithTimeZoneType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewInt96ArrayBlockBuilder(nil, positionCount)
}

// @Override
func (le *LongTimestampWithTimeZoneType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(getPackedEpochMillis(block, position))
		blockBuilder.WriteInt(getPicosOfMilli(block, position))
		blockBuilder.CloseEntry()
	}
}

// @Override
func (le *LongTimestampWithTimeZoneType) GetObject(block Block, position int32) basic.Object {
	packedEpochMillis := getPackedEpochMillis(block, position)
	picosOfMilli := getPicosOfMilli(block, position)
	return FromEpochMillisAndFraction2(UnpackMillisUtc(packedEpochMillis), picosOfMilli, UnpackZoneKey(packedEpochMillis))
}

// @Override
func (le *LongTimestampWithTimeZoneType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	timestamp := value.(*LongTimestampWithTimeZone)
	blockBuilder.WriteLong(PackDateTimeWithZone4(timestamp.GetEpochMillis(), timestamp.GetTimeZoneKey()))
	blockBuilder.WriteInt(timestamp.GetPicosOfMilli())
	blockBuilder.CloseEntry()
}

func getPackedEpochMillis(block Block, position int32) int64 {
	return block.GetLong(position, 0)
}

func getPicosOfMilli(block Block, position int32) int32 {
	return block.GetInt(position, util.INT64_BYTES)
}
