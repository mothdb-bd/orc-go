package store

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var BOOLEAN_OUTPUT_INSTANCE_SIZE int32 = util.SizeOf(&BooleanOutputStream{})

type BooleanOutputStream struct {
	// 继承
	ValueOutputStream[*BooleanStreamCheckpoint]

	byteOutputStream     *ByteOutputStream
	checkpointBitOffsets *util.ArrayList[int32]
	bitsInData           int32
	data                 int32
	closed               bool
}

func NewBooleanOutputStream(compression metadata.CompressionKind, bufferSize int32) *BooleanOutputStream {
	return NewBooleanOutputStream3(NewByteOutputStream(compression, bufferSize))
}
func NewBooleanOutputStream2(buffer *MothOutputBuffer) *BooleanOutputStream {
	return NewBooleanOutputStream3(NewByteOutputStream2(buffer))
}
func NewBooleanOutputStream3(byteOutputStream *ByteOutputStream) *BooleanOutputStream {
	bm := new(BooleanOutputStream)
	bm.checkpointBitOffsets = util.NewArrayList[int32]()

	bm.byteOutputStream = byteOutputStream
	return bm
}

func (bm *BooleanOutputStream) WriteBoolean(value bool) {
	util.CheckState(!bm.closed)
	if value {
		bm.data |= 0x1 << (7 - bm.bitsInData)
	}
	bm.bitsInData++
	if bm.bitsInData == 8 {
		bm.flushData()
	}
}

func (bm *BooleanOutputStream) writeBooleans(count int32, value bool) {
	util.CheckArgument2(count >= 0, "count is negative")
	if count == 0 {
		return
	}

	if bm.bitsInData != 0 {
		bitsToWrite := maths.MinInt32(count, 8-bm.bitsInData)
		if value {
			bm.data |= getLowBitMask(bitsToWrite) << (8 - bm.bitsInData - bitsToWrite)
		}

		bm.bitsInData += bitsToWrite
		count -= bitsToWrite
		if bm.bitsInData == 8 {
			bm.flushData()
		} else {
			// there were not enough bits to fill the current data
			util.Verify(count == 0)
			return
		}
	}

	// at this point there should be no pending data
	util.Verify(bm.bitsInData == 0)

	// write 8 bits at a time
	for count >= 8 {
		if value {
			bm.byteOutputStream.WriteByte(byte(0b1111_1111))
		} else {
			bm.byteOutputStream.WriteByte(byte(0b0000_0000))
		}
		count -= 8
	}

	// buffer remaining bits
	if count > 0 {
		if value {
			bm.data = getLowBitMask(count) << (8 - count)
		}
		bm.bitsInData = count
	}
}

func (bm *BooleanOutputStream) flushData() {
	bm.byteOutputStream.WriteByte(byte(bm.data))
	bm.data = 0
	bm.bitsInData = 0
}

// @Override
func (bm *BooleanOutputStream) RecordCheckpoint() {
	util.CheckState(!bm.closed)
	bm.byteOutputStream.RecordCheckpoint()
	bm.checkpointBitOffsets.Add(bm.bitsInData)
}

// @Override
func (bm *BooleanOutputStream) Close() {
	bm.closed = true
	if bm.bitsInData > 0 {
		bm.flushData()
	}
	bm.byteOutputStream.Close()
}

// @Override
func (bm *BooleanOutputStream) GetCheckpoints() *util.ArrayList[*BooleanStreamCheckpoint] {
	util.CheckState(bm.closed)
	booleanStreamCheckpoint := util.NewArrayList[*BooleanStreamCheckpoint]()
	byteStreamCheckpoints := bm.byteOutputStream.GetCheckpoints()
	for groupId := 0; groupId < bm.checkpointBitOffsets.Size(); groupId++ {
		checkpointBitOffset := bm.checkpointBitOffsets.Get(groupId)
		byteStreamCheckpoint := byteStreamCheckpoints.Get(groupId)
		booleanStreamCheckpoint.Add(NewBooleanStreamCheckpoint(checkpointBitOffset, byteStreamCheckpoint))
	}
	return booleanStreamCheckpoint
}

// @Override
func (bm *BooleanOutputStream) GetStreamDataOutput(columnId metadata.MothColumnId) *StreamDataOutput {
	util.CheckState(bm.closed)
	return bm.byteOutputStream.GetStreamDataOutput(columnId)
}

// @Override
func (bm *BooleanOutputStream) GetBufferedBytes() int64 {
	return bm.byteOutputStream.GetBufferedBytes()
}

// @Override
func (bm *BooleanOutputStream) GetRetainedBytes() int64 {
	return int64(BOOLEAN_OUTPUT_INSTANCE_SIZE) + bm.byteOutputStream.GetRetainedBytes()
}

// @Override
func (bm *BooleanOutputStream) Reset() {
	bm.data = 0
	bm.bitsInData = 0

	bm.closed = false
	bm.byteOutputStream.Reset()
	bm.checkpointBitOffsets.Clear()
}

func getLowBitMask(bits int32) int32 {
	return (0x1 << bits) - 1
}
