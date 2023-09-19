package slice

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	DYNAMIC_INSTANCE_SIZE int32 = util.SizeOf(&DynamicSliceOutput{})
)

type DynamicSliceOutput struct {
	// 继承
	SliceOutput

	slice         *Slice
	estimatedSize int32
}

func NewDynamicSliceOutput(estimatedSize int32) *DynamicSliceOutput {
	ct := new(DynamicSliceOutput)
	ct.slice = NewWithSize(int(estimatedSize))
	ct.estimatedSize = estimatedSize
	return ct
}

// @Override
func (ct *DynamicSliceOutput) Reset() {
	if ct.estimatedSize > 0 {
		ct.slice = NewWithSize(int(ct.estimatedSize))
	} else {
		ct.slice, _ = NewSlice()
	}
}

// @Override
func (ct *DynamicSliceOutput) Reset2(position int32) {
	util.CheckArgument2(position >= 0, "position is negative")
	ct.slice.writerIndex = int(position)
}

// @Override
func (ct *DynamicSliceOutput) Size() int32 {
	return ct.slice.SizeInt32()
}

// @Override
func (ct *DynamicSliceOutput) GetRetainedSize() int64 {
	return int64(ct.slice.GetRetainedSize()) + int64(DYNAMIC_INSTANCE_SIZE)
}

// @Override
func (ct *DynamicSliceOutput) WritableBytes() int32 {
	return ct.slice.SizeInt32()
}

// @Override
func (ct *DynamicSliceOutput) IsWritable() bool {
	return true
}

// @Override
func (ct *DynamicSliceOutput) WriteByte(b byte) error {
	ct.slice.WriteByte(b)
	return nil
}

// @Override
func (ct *DynamicSliceOutput) WriteShort(value int16) {
	ct.slice.WriteInt16LE(value)
}

// @Override
func (ct *DynamicSliceOutput) WriteInt(value int32) {
	ct.slice.WriteInt32LE(value)
}

// @Override
func (ct *DynamicSliceOutput) WriteLong(value int64) {
	ct.slice.WriteInt64LE(value)
}

// @Override
func (ct *DynamicSliceOutput) WriteFloat(value float32) {
	ct.WriteInt(int32(math.Float32bits(value)))
}

// @Override
func (ct *DynamicSliceOutput) WriteDouble(value float64) {
	ct.WriteLong(int64(math.Float64bits(value)))
}

// @Override
func (ct *DynamicSliceOutput) WriteSlice(source *Slice) {
	ct.WriteSlice2(source, 0, source.SizeInt32())
}

// @Override
func (ct *DynamicSliceOutput) WriteSlice2(source *Slice, sourceIndex int32, length int32) {
	// b := make([]byte, length)
	// source.GetBytes(b, int(sourceIndex), int(sourceIndex+length))
	// ct.slice.PutBytes(int(ct.size), b)
	ct.slice.WriteSlice(source, int(sourceIndex), int(length))
}

// @Override
func (ct *DynamicSliceOutput) WriteBytes(source []byte) {
	ct.WriteBytes2(source, 0, util.Lens(source))
}

func (ct *DynamicSliceOutput) WriteBS(source []byte) (n int, err error) {
	l := len(source)
	ct.WriteBytes2(source, 0, int32(l))
	return l, nil
}

func (ct *DynamicSliceOutput) WriteBS2(b []byte, off int32, len int32) (n int, err error) {
	ct.slice.WriteBytes(b[off : off+len])
	return int(len), nil
}

// @Override
func (ct *DynamicSliceOutput) WriteBytes2(source []byte, sourceIndex int32, length int32) {
	ct.slice.WriteBytes(source[sourceIndex : sourceIndex+length])
}

// @Override
func (ct *DynamicSliceOutput) WriteInputStream(input mothio.InputStream, length int32) {
	b := make([]byte, length)
	input.ReadBS2(b)
	ct.slice.WriteBytes(b)
}

// @Override
func (ct *DynamicSliceOutput) WriteZero(length int32) {
	if length == 0 {
		return
	}
	if length < 0 {
		panic("length must be 0 or greater than 0.")
	}
	nLong := maths.UnsignedRightShiftInt32(length, 3)
	nBytes := length & 7
	for i := nLong; i > 0; i-- {
		ct.WriteLong(0)
	}
	if nBytes == 4 {
		ct.WriteInt(0)
	} else if nBytes < 4 {
		for i := nBytes; i > 0; i-- {
			ct.WriteByte(0)
		}
	} else {
		ct.WriteInt(0)
		for i := nBytes - 4; i > 0; i-- {
			ct.WriteByte(0)
		}
	}
}

// @Override
func (ct *DynamicSliceOutput) AppendLong(value int64) SliceOutput {
	ct.WriteLong(value)
	return ct
}

// @Override
func (ct *DynamicSliceOutput) AppendDouble(value float64) SliceOutput {
	ct.WriteDouble(value)
	return ct
}

// @Override
func (ct *DynamicSliceOutput) AppendInt(value int32) SliceOutput {
	ct.WriteInt(value)
	return ct
}

// @Override
func (ct *DynamicSliceOutput) AppendShort(value int16) SliceOutput {
	ct.WriteShort(value)
	return ct
}

// @Override
func (ct *DynamicSliceOutput) AppendByte(value byte) SliceOutput {
	ct.WriteByte(value)
	return ct
}

// @Override
func (ct *DynamicSliceOutput) AppendBytes(source []byte, sourceIndex int32, length int32) SliceOutput {
	ct.WriteBytes2(source, sourceIndex, length)
	return ct
}

// @Override
func (ct *DynamicSliceOutput) AppendBytes2(source []byte) SliceOutput {
	ct.WriteBytes(source)
	return ct
}

// @Override
func (ct *DynamicSliceOutput) AppendSlice(slice *Slice) SliceOutput {
	ct.WriteSlice(slice)
	return ct
}

// @Override
func (ct *DynamicSliceOutput) Slice() *Slice {
	return ct.slice
}

// @Override
func (ct *DynamicSliceOutput) GetUnderlyingSlice() *Slice {
	return ct.slice
}

// @Override
func (ct *DynamicSliceOutput) String() string {
	return ct.ToString()
}

// @Override
func (ct *DynamicSliceOutput) ToString() string {
	builder := util.NewSB().AppendString("OutputStreamSliceOutputAdapter{")
	builder.AppendString("position=").AppendInt32(ct.Size())
	builder.AppendString("bufferSize=").AppendInt32(ct.slice.SizeInt32())
	builder.AppendInt8('}')
	return builder.String()
}
