package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type LongTimeWithTimeZoneType struct {
	TimeWithTimeZoneType
}

func NewLongTimeWithTimeZoneType(precision int32) *LongTimeWithTimeZoneType {
	le := new(LongTimeWithTimeZoneType)

	le.signature = NewTypeSignature(ST_TIME_WITH_TIME_ZONE, NumericParameter(int64(precision)))
	le.goKind = reflect.TypeOf(&LongTimeWithTimeZone{}).Kind()
	le.precision = precision

	if precision < TTZ_MAX_SHORT_PRECISION+1 || precision > TTZ_MAX_PRECISION {
		panic(fmt.Sprintf("Precision must be in the range [%d, %d]", TTZ_MAX_SHORT_PRECISION+1, TTZ_MAX_PRECISION))
	}

	le.AbstractType = *NewAbstractType(le.signature, le.goKind)
	return le
}

// @Override
func (le *LongTimeWithTimeZoneType) GetFixedSize() int32 {
	return util.INT64_BYTES + util.INT32_BYTES
}

// @Override
func (le *LongTimeWithTimeZoneType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewInt96ArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/le.GetFixedSize()))
}

// @Override
func (le *LongTimeWithTimeZoneType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return le.CreateBlockBuilder(blockBuilderStatus, expectedEntries, le.GetFixedSize())
}

// @Override
func (le *LongTimeWithTimeZoneType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewInt96ArrayBlockBuilder(nil, positionCount)
}

// @Override
func (le *LongTimeWithTimeZoneType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(getPicos(block, position))
		blockBuilder.WriteInt(getOffsetMinutes(block, position))
		blockBuilder.CloseEntry()
	}
}

// @Override
func (le *LongTimeWithTimeZoneType) GetObject(block Block, position int32) basic.Object {
	return NewLongTimeWithTimeZone(getPicos(block, position), getOffsetMinutes(block, position))
}

// @Override
func (le *LongTimeWithTimeZoneType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	timestamp := value.(*LongTimeWithTimeZone)
	blockBuilder.WriteLong(timestamp.GetPicoseconds())
	blockBuilder.WriteInt(timestamp.GetOffsetMinutes())
	blockBuilder.CloseEntry()
}

// @Override
func (te *LongTimeWithTimeZoneType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
func getPicos(block Block, position int32) int64 {
	return block.GetLong(position, 0)
}

func getOffsetMinutes(block Block, position int32) int32 {
	return block.GetInt(position, util.INT64_BYTES)
}
