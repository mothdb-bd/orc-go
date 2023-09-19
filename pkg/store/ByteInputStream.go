package store

import (
	"io"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var BYTE_IN_REPEAT_SIZE int32 = 3
var MIN_REPEAT_SIZE int32 = 3

type ByteInputStream struct {
	// 继承
	ValueInputStream[*ByteStreamCheckpoint]

	input                   *MothInputStream
	buffer                  []byte
	length                  int32
	offset                  int32
	lastReadInputCheckpoint int64
}

func NewByteInputStream(input *MothInputStream) *ByteInputStream {
	bm := new(ByteInputStream)

	bm.buffer = make([]byte, MIN_REPEAT_SIZE+127)
	bm.input = input
	bm.lastReadInputCheckpoint = input.GetCheckpoint()
	return bm
}

func (bm *ByteInputStream) readNextBlock() {
	bm.lastReadInputCheckpoint = bm.input.GetCheckpoint()
	control, err := bm.input.ReadBS()
	if err != nil && err != io.EOF {
		panic("Read past end of buffer RLE byte")
	}
	bm.offset = 0
	if (control & 0x80) == 0 {
		bm.length = int32(control) + MIN_REPEAT_SIZE
		value, err := bm.input.ReadBS()
		if err != nil && err != io.EOF {
			panic("Reading RLE byte got EOF")
		}
		util.FillArrays(bm.buffer, 0, bm.length, byte(value))
	} else {
		bm.length = 0x100 - int32(control)
		bm.input.ReadFully(bm.buffer, 0, int(bm.length))
	}
}

// @Override
func (bm *ByteInputStream) SeekToCheckpoint(checkpoint StreamCheckpoint) {
	bt := checkpoint.(*ByteStreamCheckpoint)
	if bm.lastReadInputCheckpoint == bt.GetInputStreamCheckpoint() && bt.GetOffset() <= bm.length {
		bm.offset = bt.GetOffset()
	} else {
		bm.input.SeekToCheckpoint(bt.GetInputStreamCheckpoint())
		bm.length = 0
		bm.offset = 0
		bm.Skip(int64(bt.GetOffset()))
	}
}

// @Override
func (bm *ByteInputStream) Skip(items int64) {
	for items > 0 {
		if bm.offset == bm.length {
			bm.readNextBlock()
		}
		consume := maths.Min(items, int64(bm.length-bm.offset))
		bm.offset += int32(consume)
		items -= consume
	}
}

func (bm *ByteInputStream) Next() byte {
	if bm.offset == bm.length {
		bm.readNextBlock()
	}
	b := bm.buffer[bm.offset]
	bm.offset++
	return b
}

func (bm *ByteInputStream) Next2(items int32) []byte {
	values := make([]byte, items)
	bm.Next3(values, items)
	return values
}

func (bm *ByteInputStream) Next3(values []byte, items int32) {
	outputOffset := util.INT32_ZERO
	for outputOffset < items {
		if bm.offset == bm.length {
			bm.readNextBlock()
		}
		if bm.length == 0 {
			panic("Unexpected end of stream")
		}
		chunkSize := maths.MinInt32(items-outputOffset, bm.length-bm.offset)
		util.CopyBytes(bm.buffer, bm.offset, values, outputOffset, chunkSize)

		outputOffset += chunkSize
		bm.offset += chunkSize
	}
}
