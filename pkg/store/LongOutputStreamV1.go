package store

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	LONG_OUTPUTV1_INSTANCE_SIZE           int32 = util.SizeOf(&LongOutputStreamV1{})
	LONG_OUTPUTV1_MIN_REPEAT_SIZE         int32 = 3
	LONG_OUTPUTV1_UNMATCHABLE_DELTA_VALUE int64 = math.MaxInt64
	LONG_OUTPUTV1_MAX_DELTA               int32 = 127
	LONG_OUTPUTV1_MIN_DELTA               int32 = -128
)

type LongOutputStreamV1 struct {

	// 继承
	LongOutputStream

	streamKind     metadata.StreamKind
	buffer         *MothOutputBuffer
	checkpoints    *util.ArrayList[LongStreamCheckpoint]
	sequenceBuffer []int64
	signed         bool
	size           int32
	runCount       int32
	lastValue      int64
	lastDelta      int64
	closed         bool
}

func NewLongOutputStreamV1(compression metadata.CompressionKind, bufferSize int32, signed bool, streamKind metadata.StreamKind) *LongOutputStreamV1 {
	l1 := new(LongOutputStreamV1)

	l1.checkpoints = util.NewArrayList[LongStreamCheckpoint]()
	l1.sequenceBuffer = make([]int64, 128)
	l1.lastDelta = LONG_OUTPUTV1_UNMATCHABLE_DELTA_VALUE

	l1.streamKind = streamKind
	l1.buffer = NewMothOutputBuffer(compression, bufferSize)
	l1.signed = signed
	return l1
}

// @Override
func (l1 *LongOutputStreamV1) WriteLong(value int64) {
	util.CheckState(!l1.closed)
	if l1.size == util.Lens(l1.sequenceBuffer) {
		l1.flushSequence()
	}
	l1.sequenceBuffer[l1.size] = value
	l1.size++
	delta := value - l1.lastValue
	if delta == l1.lastDelta {
		l1.runCount++
		if l1.runCount == LONG_OUTPUTV1_MIN_REPEAT_SIZE && l1.size > LONG_OUTPUTV1_MIN_REPEAT_SIZE {
			l1.flushLiteralSequence(l1.size - LONG_OUTPUTV1_MIN_REPEAT_SIZE)
			l1.size = LONG_OUTPUTV1_MIN_REPEAT_SIZE
		}
	} else {
		if l1.runCount >= LONG_OUTPUTV1_MIN_REPEAT_SIZE {
			l1.flushRleSequence(l1.runCount)
			l1.sequenceBuffer[0] = value
			l1.size = 1
		}
		if l1.size == 1 || !isValidDelta(delta) {
			l1.runCount = 1
			l1.lastDelta = LONG_OUTPUTV1_UNMATCHABLE_DELTA_VALUE
		} else {
			l1.runCount = 2
			l1.lastDelta = delta
		}
	}
	l1.lastValue = value
}

func isValidDelta(delta int64) bool {
	return delta >= int64(LONG_OUTPUTV1_MIN_DELTA) && delta <= int64(LONG_OUTPUTV1_MAX_DELTA)
}

func (l1 *LongOutputStreamV1) flushSequence() {
	if l1.size == 0 {
		return
	}
	if l1.runCount >= LONG_OUTPUTV1_MIN_REPEAT_SIZE {
		l1.flushRleSequence(l1.runCount)
	} else {
		l1.flushLiteralSequence(l1.size)
	}
	l1.size = 0
	l1.runCount = 0
	l1.lastValue = 0
	l1.lastDelta = LONG_OUTPUTV1_UNMATCHABLE_DELTA_VALUE
}

func (l1 *LongOutputStreamV1) flushLiteralSequence(literalCount int32) {
	util.Verify(literalCount > 0)
	l1.buffer.WriteByte(byte(-literalCount))
	for i := util.INT32_ZERO; i < literalCount; i++ {
		WriteVLong(l1.buffer, l1.sequenceBuffer[i], l1.signed)
	}
}

func (l1 *LongOutputStreamV1) flushRleSequence(runCount int32) {
	util.Verify(runCount > 0)
	l1.buffer.WriteByte(byte(l1.runCount - LONG_OUTPUTV1_MIN_REPEAT_SIZE))
	l1.buffer.WriteByte(byte(l1.lastDelta))
	totalDeltaSize := l1.lastDelta * int64(l1.runCount-1)
	sequenceStartValue := l1.lastValue - totalDeltaSize
	WriteVLong(l1.buffer, sequenceStartValue, l1.signed)
}

// @Override
func (l1 *LongOutputStreamV1) RecordCheckpoint() {
	util.CheckState(!l1.closed)
	l1.checkpoints.Add(NewLongStreamV1Checkpoint(l1.size, l1.buffer.GetCheckpoint()))
}

// @Override
func (l1 *LongOutputStreamV1) Close() {
	l1.closed = true
	l1.flushSequence()
	l1.buffer.Close()
}

// @Override
func (l1 *LongOutputStreamV1) GetCheckpoints() *util.ArrayList[LongStreamCheckpoint] {
	util.CheckState(l1.closed)
	return l1.checkpoints
}

// @Override
func (l1 *LongOutputStreamV1) GetStreamDataOutput(columnId metadata.MothColumnId) *StreamDataOutput {
	return NewStreamDataOutput2(l1.buffer.WriteDataTo, metadata.NewStream(columnId, l1.streamKind, util.Int32Exact(l1.buffer.GetOutputDataSize()), true))
}

// @Override
func (l1 *LongOutputStreamV1) GetBufferedBytes() int64 {
	return l1.buffer.EstimateOutputDataSize() + int64(util.INT64_BYTES*l1.size)
}

// @Override
func (l1 *LongOutputStreamV1) GetRetainedBytes() int64 {
	return int64(LONG_OUTPUTV1_INSTANCE_SIZE) + l1.buffer.GetRetainedSize() + util.SizeOfInt64(l1.sequenceBuffer)
}

// @Override
func (l1 *LongOutputStreamV1) Reset() {
	l1.size = 0
	l1.runCount = 0
	l1.lastValue = 0
	l1.lastDelta = LONG_OUTPUTV1_UNMATCHABLE_DELTA_VALUE
	l1.closed = false
	l1.buffer.Reset()
	l1.checkpoints.Clear()
}
