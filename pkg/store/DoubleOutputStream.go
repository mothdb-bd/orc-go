package store

import (
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var DOUBLE_OUT_INSTANCE_SIZE int32 = util.SizeOf(&DoubleOutputStream{})

type DoubleOutputStream struct {
	// 继承
	ValueOutputStream[*DoubleStreamCheckpoint]

	buffer      *MothOutputBuffer
	checkpoints *util.ArrayList[*DoubleStreamCheckpoint]
	closed      bool
}

func NewDoubleOutputStream(compression metadata.CompressionKind, bufferSize int32) *DoubleOutputStream {
	dm := new(DoubleOutputStream)
	dm.checkpoints = util.NewArrayList[*DoubleStreamCheckpoint]()
	dm.buffer = NewMothOutputBuffer(compression, bufferSize)
	return dm
}

func (dm *DoubleOutputStream) WriteDouble(value float64) {
	util.CheckState(!dm.closed)
	dm.buffer.WriteDouble(value)
}

// @Override
func (dm *DoubleOutputStream) Close() {
	dm.closed = true
	dm.buffer.Close()
}

// @Override
func (dm *DoubleOutputStream) RecordCheckpoint() {
	util.CheckState(!dm.closed)
	dm.checkpoints.Add(NewDoubleStreamCheckpoint(dm.buffer.GetCheckpoint()))
}

// @Override
func (dm *DoubleOutputStream) GetCheckpoints() *util.ArrayList[*DoubleStreamCheckpoint] {
	util.CheckState(dm.closed)
	return dm.checkpoints
}

// @Override
func (dm *DoubleOutputStream) GetStreamDataOutput(columnId metadata.MothColumnId) *StreamDataOutput {
	return NewStreamDataOutput2(dm.buffer.WriteDataTo, metadata.NewStream(columnId, metadata.DATA, util.Int32Exact(dm.buffer.GetOutputDataSize()), false))
}

// @Override
func (dm *DoubleOutputStream) GetBufferedBytes() int64 {
	return dm.buffer.EstimateOutputDataSize()
}

// @Override
func (dm *DoubleOutputStream) GetRetainedBytes() int64 {
	return int64(DOUBLE_OUT_INSTANCE_SIZE) + dm.buffer.GetRetainedSize()
}

// @Override
func (dm *DoubleOutputStream) Reset() {
	dm.closed = false
	dm.buffer.Reset()
	dm.checkpoints.Clear()
}
