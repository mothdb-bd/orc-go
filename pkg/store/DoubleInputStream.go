package store

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var DOUBLE_IN_BUFFER_SIZE int32 = 128

type DoubleInputStream struct {
	// 继承
	ValueInputStream[*DoubleStreamCheckpoint]

	input *MothInputStream
	slice *slice.Slice
}

func NewDoubleInputStream(input *MothInputStream) *DoubleInputStream {
	dm := new(DoubleInputStream)

	buffer := make([]byte, util.FLOAT64_BYTES*DOUBLE_IN_BUFFER_SIZE)
	dm.slice = slice.NewBaseBuf(buffer)
	dm.input = input
	return dm
}

// @Override
func (dm *DoubleInputStream) SeekToCheckpoint(checkpoint StreamCheckpoint) {
	dt := checkpoint.(*DoubleStreamCheckpoint)
	dm.input.SeekToCheckpoint(dt.GetInputStreamCheckpoint())
}

// @Override
func (dm *DoubleInputStream) Skip(items int64) {
	length := items * util.FLOAT64_BYTES
	dm.input.SkipFully(length)
}

func (dm *DoubleInputStream) Next() float64 {
	dm.input.ReadFully2(dm.slice, 0, util.FLOAT64_BYTES)
	f, _ := dm.slice.ReadFloat64LE()
	return f
}

func (dm *DoubleInputStream) Next2(values []int64, items int32) {

	bl := int(items * util.FLOAT64_BYTES)
	b := make([]byte, bl)
	s := slice.NewBaseBuf(b)
	dm.input.ReadFully2(s, 0, bl)

	for i := util.INT32_ZERO; i < items; i++ {
		v, _ := s.ReadInt64LE()
		values[i] = v
	}
}
