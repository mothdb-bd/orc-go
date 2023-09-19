package slice

import (
	"fmt"
	"io"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

// func final class BasicSliceInput
//
//	extends FixedLengthSliceInput
type BasicSliceInput struct {
	// 继承
	FixedLengthSliceInput

	slice    *Slice
	position int64
}

var (
	BASIC_INSTANCE_SIZE int32 = util.SizeOf(&BasicSliceInput{})
)

func NewBasicSliceInput(slice *Slice) *BasicSliceInput {
	bt := new(BasicSliceInput)
	bt.slice = slice
	return bt
}

func (bt *BasicSliceInput) GetReader() io.Reader {
	return bt
}

func (bt *BasicSliceInput) Read(p []byte) (n int, err error) {
	l, _ := bt.slice.ReadBytes(p)
	if bt.slice.IsReadable() {
		return l, nil
	} else {
		return l, io.EOF
	}
}

// @Override
func (bt *BasicSliceInput) Length() int64 {
	return bt.slice.LenInt64()
}

// @Override
func (bt *BasicSliceInput) Position() int64 {
	return bt.position
}

func CheckPositionIndex(index, size int64) int64 {
	return CheckPositionIndex2(index, size, "index")
}

func CheckPositionIndex2(index, size int64, desc string) int64 {
	// Carefully optimized for execution by hotspot (explanatory comment above)
	if index < 0 || index > size {
		panic(fmt.Sprintf("index is %d ,size is %d, out of bounds ", index, size))
	}
	return index
}

// @Override
func (bt *BasicSliceInput) SetPosition(position int64) {
	CheckPositionIndex(position, bt.Length())
	bt.position = position
}

// @Override
func (bt *BasicSliceInput) IsReadable() bool {
	return bt.position < bt.slice.LenInt64()
}

// @Override
func (bt *BasicSliceInput) Available() int32 {
	return int32(bt.slice.LenInt64() - bt.position)
}

// @Override
func (bt *BasicSliceInput) ReadBoolean() bool {
	return bt.ReadByte() != 0
}

// @Override
func (bt *BasicSliceInput) ReadBS() (byte, error) {
	if bt.position >= bt.slice.LenInt64() {
		return 0, io.EOF
	}
	result, _ := bt.slice.ReadByte()
	bt.position++
	return result, nil
}

// @Override
func (bt *BasicSliceInput) ReadByte() byte {
	value, err := bt.ReadBS()
	if err == io.EOF {
		panic("index out of bounds")
	}
	return value
}

// @Override
func (bt *BasicSliceInput) ReadUnsignedByte() uint8 {
	if bt.position >= bt.slice.LenInt64() {
		panic("position out of bounds")
	}
	result, _ := bt.slice.ReadUInt8()
	bt.position++
	return result
}

// @Override
func (bt *BasicSliceInput) ReadShort() int16 {
	v, err := bt.slice.ReadInt16LE()
	if err == nil {
		bt.position += util.INT16_BYTES
		return v
	} else {
		panic(err.Error())
	}
}

// @Override
func (bt *BasicSliceInput) ReadUnsignedShort() uint16 {
	v, err := bt.slice.ReadUInt16LE()
	if err == nil {
		bt.position += util.INT16_BYTES
		return v
	} else {
		panic(err.Error())
	}
}

// @Override
func (bt *BasicSliceInput) ReadInt() int32 {
	v, err := bt.slice.ReadInt32LE()
	if err == nil {
		bt.position += util.INT32_BYTES
		return v
	} else {
		panic(err.Error())
	}

}

// @Override
func (bt *BasicSliceInput) ReadLong() int64 {
	v, err := bt.slice.ReadInt64LE()
	if err == nil {
		bt.position += util.INT64_BYTES
		return v
	} else {
		panic(err.Error())
	}
}

// @Override
func (bt *BasicSliceInput) ReadFloat() float32 {
	v, err := bt.slice.ReadFloat32LE()
	if err == nil {
		bt.position += util.FLOAT32_BYTES
		return v
	} else {
		panic(err.Error())
	}
}

// @Override
func (bt *BasicSliceInput) ReadDouble() float64 {
	v, err := bt.slice.ReadFloat64LE()
	if err == nil {
		bt.position += util.FLOAT64_BYTES
		return v
	} else {
		panic(err.Error())
	}
}

// @Override
func (bt *BasicSliceInput) ReadSlice(length int32) *Slice {
	if length == 0 {
		return EMPTY_SLICE
	}
	newSlice, err := bt.slice.ReadSlice(int(length))
	if err == nil && newSlice != nil {
		bt.position += newSlice.LenInt64()
		return newSlice
	} else {
		panic(err)
	}
}

// @Override
func (bt *BasicSliceInput) ReadBS3(destination []byte, destinationIndex int, length int) (n int, err error) {
	if length == 0 {
		return 0, nil
	}

	length = maths.MinInt(length, int(bt.Available()))
	if length == 0 {
		return 0, io.EOF
	}
	re, err := bt.slice.ReadBytes(destination[destinationIndex : destinationIndex+length])
	if err == nil {
		bt.position += int64(re)
	}
	return re, err
}

func (bt *BasicSliceInput) ReadBS2(b []byte) (n int, err error) {
	return bt.ReadBS3(b, 0, len(b))
}

// @Override
func (bt *BasicSliceInput) ReadBytes2(destination []byte, destinationIndex int32, length int32) {

	_, err := bt.ReadBS3(destination, int(destinationIndex), int(length))
	if err != nil {
		panic(err)
	}
}

// @Override
func (bt *BasicSliceInput) ReadSlice4(destination *Slice, destinationIndex int32, length int32) {
	b := make([]byte, length)
	n, err := bt.slice.ReadBytes(b)
	if err == nil {
		destination.WriteBytes(b[0:n])
		bt.position += int64(length)
	} else {
		panic(err)
	}
}

// @Override
func (bt *BasicSliceInput) ReadOutputStream(out mothio.OutputStream, length int32) {
	b := make([]byte, length)
	n, err := bt.slice.ReadBytes(b)
	if err == nil {
		out.WriteBS(b[:n])
		bt.position += int64(n)
	} else {
		panic(err)
	}
}

// @Override
func (bt *BasicSliceInput) Skip(length int64) int64 {
	length = maths.Min(length, int64(bt.Available()))
	n, err := bt.slice.Skip(int(length))
	if err == nil {
		bt.position += int64(n)
		return int64(n)
	} else {
		panic(err)
	}
}

// @Override
func (bt *BasicSliceInput) SkipBytes(length int32) int32 {
	length = maths.MinInt32(length, bt.Available())
	n, err := bt.slice.Skip(int(length))
	if err == nil {
		bt.position += int64(n)
		return int32(n)
	} else {
		panic(err)
	}
}

// @Override
func (bt *BasicSliceInput) GetRetainedSize() int64 {
	return int64(BASIC_INSTANCE_SIZE + bt.slice.GetRetainedSize())
}

/**
* Returns a slice of this buffer's readable bytes. Modifying the content
* of the returned buffer or this buffer affects each other's content
* while they maintain separate indexes and marks.  This method is
* identical to {@code buf.slice(buf.position(), buf.available()())}.
* This method does not modify {@code position} or {@code writerIndex} of
* this buffer.
 */
func (bt *BasicSliceInput) Slice() *Slice {
	s, _ := bt.slice.MakeSlice(int(bt.position), int(bt.slice.LenInt64()-bt.position))
	return s
}

func (bt *BasicSliceInput) Remaining() int64 {
	return bt.Length() - bt.Position()
}

// @Override
func (bt *BasicSliceInput) String() string {
	builder := util.NewSB().AppendString("BasicSliceInput{")
	builder.AppendString("position=").AppendInt64(bt.position)
	builder.AppendString(", capacity=").AppendInt32(bt.slice.Length())
	builder.AppendInt8('}')
	return builder.String()
}
