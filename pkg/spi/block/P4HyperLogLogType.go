package block

import "github.com/mothdb-bd/orc-go/pkg/slice"

var HYPER_LOG_LOG *P4HyperLogLogType = NewP4HyperLogLogType()

type P4HyperLogLogType struct {
	// 继承
	AbstractVariableWidthType
}

func NewP4HyperLogLogType() *P4HyperLogLogType {
	pe := new(P4HyperLogLogType)
	pe.signature = NewTypeSignature(ST_P4_HYPER_LOG_LOG)
	pe.goKind = slice.SLICE_KIND

	pe.AbstractType = *NewAbstractType(pe.signature, pe.goKind)
	return pe
}

// @Override
func (pe *P4HyperLogLogType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	HYPER_LOG_LOG.AppendTo(block, position, blockBuilder)
}

// @Override
func (pe *P4HyperLogLogType) GetSlice(block Block, position int32) *slice.Slice {
	return HYPER_LOG_LOG.GetSlice(block, position)
}

// @Override
func (pe *P4HyperLogLogType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	HYPER_LOG_LOG.WriteSlice(blockBuilder, value)
}

// @Override
func (pe *P4HyperLogLogType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	HYPER_LOG_LOG.WriteSlice2(blockBuilder, value, offset, length)
}
