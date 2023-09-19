package store

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	BYTE_INSTANCE_SIZE     int32 = util.SizeOf(&ByteOutputStream{})
	BYTE_MIN_REPEAT_SIZE   int32 = 3
	BYTE_UNMATCHABLE_VALUE int32 = math.MaxInt32
)

type ByteOutputStream struct {
	//
	ValueOutputStream[*ByteStreamCheckpoint]

	buffer         *MothOutputBuffer
	checkpoints    *util.ArrayList[*ByteStreamCheckpoint]
	sequenceBuffer []byte
	size           int32
	runCount       int32
	lastValue      int32
	closed         bool
}

func NewByteOutputStream(compression metadata.CompressionKind, bufferSize int32) *ByteOutputStream {
	return NewByteOutputStream2(NewMothOutputBuffer(compression, bufferSize))
}
func NewByteOutputStream2(buffer *MothOutputBuffer) *ByteOutputStream {
	bm := new(ByteOutputStream)
	bm.checkpoints = util.NewArrayList[*ByteStreamCheckpoint]()
	bm.sequenceBuffer = make([]byte, 128)
	bm.lastValue = BYTE_UNMATCHABLE_VALUE

	bm.buffer = buffer
	return bm
}

func (bm *ByteOutputStream) WriteByte(value byte) {
	util.CheckState(!bm.closed)
	if bm.size == util.Lens(bm.sequenceBuffer) {
		bm.flushSequence()
	}
	if value == byte(bm.lastValue) {
		bm.runCount++
	} else {
		if bm.runCount >= MIN_REPEAT_SIZE {
			bm.flushSequence()
		}
		bm.runCount = 1
	}
	bm.sequenceBuffer[bm.size] = value
	bm.size++
	if bm.runCount == MIN_REPEAT_SIZE && bm.size > BYTE_MIN_REPEAT_SIZE {
		bm.size -= MIN_REPEAT_SIZE
		bm.runCount = 0
		bm.flushSequence()
		bm.runCount = MIN_REPEAT_SIZE
		bm.size = MIN_REPEAT_SIZE
	}
	bm.lastValue = int32(value)
}

func (bm *ByteOutputStream) flushSequence() {
	if bm.size == 0 {
		return
	}
	if bm.runCount >= MIN_REPEAT_SIZE {
		bm.buffer.WriteByte(byte(bm.runCount - BYTE_MIN_REPEAT_SIZE))
		bm.buffer.WriteByte(byte(bm.lastValue))
	} else {
		bm.buffer.WriteByte(-byte(bm.size))
		for i := util.INT32_ZERO; i < bm.size; i++ {
			bm.buffer.WriteByte(bm.sequenceBuffer[i])
		}
	}
	bm.size = 0
	bm.runCount = 0
	bm.lastValue = BYTE_UNMATCHABLE_VALUE
}

// @Override
func (bm *ByteOutputStream) RecordCheckpoint() {
	util.CheckState(!bm.closed)
	bm.checkpoints.Add(NewByteStreamCheckpoint(bm.size, bm.buffer.GetCheckpoint()))
}

// @Override
func (bm *ByteOutputStream) Close() {
	bm.closed = true
	bm.flushSequence()
	bm.buffer.Close()
}

// @Override
func (bm *ByteOutputStream) GetCheckpoints() *util.ArrayList[*ByteStreamCheckpoint] {
	util.CheckState(bm.closed)
	return bm.checkpoints
}

// @Override
func (bm *ByteOutputStream) GetStreamDataOutput(columnId metadata.MothColumnId) *StreamDataOutput {
	return NewStreamDataOutput2(bm.buffer.WriteDataTo, metadata.NewStream(columnId, metadata.DATA, util.Int32Exact(bm.buffer.GetOutputDataSize()), false))
}

// @Override
func (bm *ByteOutputStream) GetBufferedBytes() int64 {
	return bm.buffer.EstimateOutputDataSize() + int64(bm.size)
}

// @Override
func (bm *ByteOutputStream) GetRetainedBytes() int64 {
	return int64(BYTE_INSTANCE_SIZE) + bm.buffer.GetRetainedSize() + util.SizeOfInt64(bm.sequenceBuffer)
}

// @Override
func (bm *ByteOutputStream) Reset() {
	bm.size = 0
	bm.runCount = 0
	bm.lastValue = BYTE_UNMATCHABLE_VALUE
	bm.closed = false
	bm.buffer.Reset()
	bm.checkpoints.Clear()
}
