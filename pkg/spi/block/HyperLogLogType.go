package block

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
)

var HyperLogLogTypeHYPER_LOG_LOG *HyperLogLogType = NewHyperLogLogType()

type HyperLogLogType struct {
	// 继承
	AbstractVariableWidthType
}

func NewHyperLogLogType() *HyperLogLogType {
	he := new(HyperLogLogType)
	he.signature = NewTypeSignature(ST_HYPER_LOG_LOG)
	he.goKind = slice.SLICE_KIND
	he.AbstractType = *NewAbstractType(he.signature, he.goKind)
	return he
}

// @Override
func (he *HyperLogLogType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		block.WriteBytesTo(position, 0, block.GetSliceLength(position), blockBuilder)
		blockBuilder.CloseEntry()
	}
}

// @Override
func (he *HyperLogLogType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, block.GetSliceLength(position))
}

// @Override
func (he *HyperLogLogType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	he.WriteSlice2(blockBuilder, value, 0, int32(value.Size()))
}

// @Override
func (he *HyperLogLogType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	blockBuilder.WriteBytes(value, offset, length).CloseEntry()
}
