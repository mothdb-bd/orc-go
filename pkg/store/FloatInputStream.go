package store

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var FLOAT_BUFFER_SIZE int32 = 128

type FloatInputStream struct {
	// 继承
	ValueInputStream[*FloatStreamCheckpoint]

	input *MothInputStream
	slice *slice.Slice
}

func NewFloatInputStream(input *MothInputStream) *FloatInputStream {
	fm := new(FloatInputStream)
	buffer := make([]byte, util.FLOAT64_BYTES*FLOAT_BUFFER_SIZE)
	fm.slice = slice.NewBaseBuf(buffer)
	fm.input = input
	return fm
}

// @Override
func (fm *FloatInputStream) SeekToCheckpoint(checkpoint StreamCheckpoint) {
	ft := checkpoint.(*FloatStreamCheckpoint)
	fm.input.SeekToCheckpoint(ft.GetInputStreamCheckpoint())
}

// @Override
func (fm *FloatInputStream) Skip(items int64) {
	length := items * util.FLOAT32_BYTES
	fm.input.SkipFully(length)
}

func (fm *FloatInputStream) Next() float32 {
	fm.input.ReadFully2(fm.slice, 0, util.FLOAT32_BYTES)
	f, _ := fm.slice.ReadFloat32LE()
	return f
}

func (fm *FloatInputStream) Next2(values []int32, items int32) {
	bl := int(items * util.FLOAT32_BYTES)
	b := make([]byte, bl)
	s := slice.NewBaseBuf(b)
	fm.input.ReadFully2(s, 0, bl)

	for i := util.INT32_ZERO; i < items; i++ {
		v, _ := s.ReadInt32LE()
		values[i] = v
	}
}
