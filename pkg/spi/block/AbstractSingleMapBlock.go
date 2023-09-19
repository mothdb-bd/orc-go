package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type AbstractSingleMapBlock struct {
	Block // 继承block

}

// abstract
func (ak *AbstractSingleMapBlock) getOffset() int32 {
	return 0
}

// abstract
func (ak *AbstractSingleMapBlock) getRawKeyBlock() Block {
	return nil
}

func (ak *AbstractSingleMapBlock) getRawValueBlock() Block {
	return nil
}

// @Override
func (ak *AbstractSingleMapBlock) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList(ak.getRawKeyBlock(), ak.getRawValueBlock())
}

func (ak *AbstractSingleMapBlock) getAbsolutePosition(position int32) int32 {
	if position < 0 || position >= ak.GetPositionCount() {
		panic("position is not valid")
	}
	return position + ak.getOffset()
}

// @Override
func (ak *AbstractSingleMapBlock) IsNull(position int32) bool {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		if ak.getRawKeyBlock().IsNull(position / 2) {
			panic("Map key is null")
		}
		return false
	} else {
		return ak.getRawValueBlock().IsNull(position / 2)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) GetByte(position int32, offset int32) byte {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().GetByte(position/2, offset)
	} else {
		return ak.getRawValueBlock().GetByte(position/2, offset)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) GetShort(position int32, offset int32) int16 {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().GetShort(position/2, offset)
	} else {
		return ak.getRawValueBlock().GetShort(position/2, offset)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) GetInt(position int32, offset int32) int32 {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().GetInt(position/2, offset)
	} else {
		return ak.getRawValueBlock().GetInt(position/2, offset)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) GetLong(position int32, offset int32) int64 {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().GetLong(position/2, offset)
	} else {
		return ak.getRawValueBlock().GetLong(position/2, offset)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().GetSlice(position/2, offset, length)
	} else {
		return ak.getRawValueBlock().GetSlice(position/2, offset, length)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) GetSliceLength(position int32) int32 {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().GetSliceLength(position / 2)
	} else {
		return ak.getRawValueBlock().GetSliceLength(position / 2)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) CompareTo(position int32, offset int32, length int32, otherBlock Block, otherPosition int32, otherOffset int32, otherLength int32) int32 {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().CompareTo(position/2, offset, length, otherBlock, otherPosition, otherOffset, otherLength)
	} else {
		return ak.getRawValueBlock().CompareTo(position/2, offset, length, otherBlock, otherPosition, otherOffset, otherLength)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().BytesEqual(position/2, offset, otherSlice, otherOffset, length)
	} else {
		return ak.getRawValueBlock().BytesEqual(position/2, offset, otherSlice, otherOffset, length)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().BytesCompare(position/2, offset, length, otherSlice, otherOffset, otherLength)
	} else {
		return ak.getRawValueBlock().BytesCompare(position/2, offset, length, otherSlice, otherOffset, otherLength)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		ak.getRawKeyBlock().WriteBytesTo(position/2, offset, length, blockBuilder)
	} else {
		ak.getRawValueBlock().WriteBytesTo(position/2, offset, length, blockBuilder)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().Equals(position/2, offset, otherBlock, otherPosition, otherOffset, length)
	} else {
		return ak.getRawValueBlock().Equals(position/2, offset, otherBlock, otherPosition, otherOffset, length)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) Hash(position int32, offset int32, length int32) int64 {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().Hash(position/2, offset, length)
	} else {
		return ak.getRawValueBlock().Hash(position/2, offset, length)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) GetObject(position int32, clazz reflect.Type) basic.Object {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().GetObject(position/2, clazz)
	} else {
		return ak.getRawValueBlock().GetObject(position/2, clazz)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) GetSingleValueBlock(position int32) Block {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().GetSingleValueBlock(position / 2)
	} else {
		return ak.getRawValueBlock().GetSingleValueBlock(position / 2)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	position = ak.getAbsolutePosition(position)
	if position%2 == 0 {
		return ak.getRawKeyBlock().GetEstimatedDataSizeForStats(position / 2)
	} else {
		return ak.getRawValueBlock().GetEstimatedDataSizeForStats(position / 2)
	}
}

// @Override
func (ak *AbstractSingleMapBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	panic("Unsupported region size")
}

// @Override
func (ak *AbstractSingleMapBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	panic("Unsupported positions size")
}

// @Override
func (ak *AbstractSingleMapBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	panic("Unsupported positions copy")
}

// @Override
func (ak *AbstractSingleMapBlock) GetRegion(positionOffset int32, length int32) Block {
	panic("Unsupported region get")
}

// @Override
func (ak *AbstractSingleMapBlock) CopyRegion(position int32, length int32) Block {
	panic("Unsupported region copy")
}
