package store

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var BYTE_ARRAY_OUTPUT_INSTANCE_SIZE int32 = util.SizeOf(&ByteArrayOutputStream{})

type ByteArrayOutputStream struct {
	// 继承
	ValueOutputStream[*ByteArrayStreamCheckpoint]

	buffer      *MothOutputBuffer
	checkpoints *util.ArrayList[*ByteArrayStreamCheckpoint]
	streamKind  metadata.StreamKind
	closed      bool
}

func NewByteArrayOutputStream(compression metadata.CompressionKind, bufferSize int32) *ByteArrayOutputStream {
	return NewByteArrayOutputStream2(compression, bufferSize, metadata.DATA)
}
func NewByteArrayOutputStream2(compression metadata.CompressionKind, bufferSize int32, streamKind metadata.StreamKind) *ByteArrayOutputStream {
	bm := new(ByteArrayOutputStream)
	bm.checkpoints = util.NewArrayList[*ByteArrayStreamCheckpoint]()

	bm.buffer = NewMothOutputBuffer(compression, bufferSize)
	bm.streamKind = streamKind
	return bm
}

func (bm *ByteArrayOutputStream) WriteSlice(value *slice.Slice) {
	util.CheckState(!bm.closed)
	bm.buffer.WriteSlice(value)
}

// @Override
func (bm *ByteArrayOutputStream) Close() {
	bm.closed = true
	bm.buffer.Close()
}

// @Override
func (bm *ByteArrayOutputStream) RecordCheckpoint() {
	util.CheckState(!bm.closed)
	bm.checkpoints.Add(NewByteArrayStreamCheckpoint(bm.buffer.GetCheckpoint()))
}

// @Override
func (bm *ByteArrayOutputStream) GetCheckpoints() *util.ArrayList[*ByteArrayStreamCheckpoint] {
	util.CheckState(bm.closed)
	return bm.checkpoints
}

// @Override
func (bm *ByteArrayOutputStream) GetStreamDataOutput(columnId metadata.MothColumnId) *StreamDataOutput {
	return NewStreamDataOutput2(bm.buffer.WriteDataTo, metadata.NewStream(columnId, bm.streamKind, util.Int32Exact(bm.buffer.GetOutputDataSize()), false))
}

// @Override
func (bm *ByteArrayOutputStream) GetBufferedBytes() int64 {
	return bm.buffer.EstimateOutputDataSize()
}

// @Override
func (bm *ByteArrayOutputStream) GetRetainedBytes() int64 {
	return int64(BYTE_ARRAY_OUTPUT_INSTANCE_SIZE) + bm.buffer.GetRetainedSize()
}

// @Override
func (bm *ByteArrayOutputStream) Reset() {
	bm.closed = false
	bm.buffer.Reset()
	bm.checkpoints.Clear()
}
