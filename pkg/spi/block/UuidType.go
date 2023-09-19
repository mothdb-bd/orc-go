package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var UuidTypeUUID *UuidType = NewUuidType()

type UuidType struct {
	// 继承
	AbstractType
	// 继承
	FixedWidthType
}

func NewUuidType() *UuidType {
	ue := new(UuidType)
	ue.signature = NewTypeSignature(ST_UUID)
	ue.goKind = slice.SLICE_KIND

	ue.AbstractType = *NewAbstractType(ue.signature, ue.goKind)
	return ue
}

// @Override
func (ue *UuidType) GetFixedSize() int32 {
	return INT128_BYTES
}

// @Override
func (ue *UuidType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewInt128ArrayBlockBuilder(blockBuilderStatus, maths.MaxInt32(expectedEntries, maxBlockSizeInBytes/ue.GetFixedSize()))
}

// @Override
func (ue *UuidType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return ue.CreateBlockBuilder(blockBuilderStatus, expectedEntries, ue.GetFixedSize())
}

// @Override
func (ue *UuidType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewInt128ArrayBlockBuilder(nil, positionCount)
}

// @Override
func (ue *UuidType) IsComparable() bool {
	return true
}

// @Override
func (ue *UuidType) IsOrderable() bool {
	return true
}

// @Override
func (ue *UuidType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(block.GetLong(position, 0))
		blockBuilder.WriteLong(block.GetLong(position, util.INT64_BYTES))
		blockBuilder.CloseEntry()
	}
}

// @Override
func (ue *UuidType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	ue.WriteSlice2(blockBuilder, value, 0, int32(value.Size()))
}

// @Override
func (ue *UuidType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	if length != INT128_BYTES {
		panic(fmt.Sprintf("Expected entry size to be exactly %d but was %d", INT128_BYTES, length))
	}
	h, _ := value.GetInt64LE(int(offset))
	l, _ := value.GetInt64LE(int(offset + util.INT64_BYTES))
	blockBuilder.WriteLong(h)
	blockBuilder.WriteLong(l)
	blockBuilder.CloseEntry()
}

// @Override
func (ue *UuidType) GetSlice(block Block, position int32) *slice.Slice {
	h := block.GetLong(position, 0)
	l := block.GetLong(position, util.INT64_BYTES)
	s := slice.NewWithSize(util.INT64_BYTES * 2)

	s.WriteInt64BE(h)
	s.WriteInt64BE(l)
	return s
}
