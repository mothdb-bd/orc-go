package store

import (
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var FLOAT_OUT_INSTANCE_SIZE int32 = util.SizeOf(&FloatOutputStream{})

type FloatOutputStream struct {
	//继承
	ValueOutputStream[*FloatStreamCheckpoint]

	buffer      *MothOutputBuffer
	checkpoints *util.ArrayList[*FloatStreamCheckpoint]
	closed      bool
}

func NewFloatOutputStream(compression metadata.CompressionKind, bufferSize int32) *FloatOutputStream {
	fm := new(FloatOutputStream)
	fm.checkpoints = util.NewArrayList[*FloatStreamCheckpoint]()
	fm.buffer = NewMothOutputBuffer(compression, bufferSize)
	return fm
}

func (fm *FloatOutputStream) WriteFloat(value float32) {
	util.CheckState(!fm.closed)
	fm.buffer.WriteFloat(value)
}

// @Override
func (fm *FloatOutputStream) Close() {
	fm.closed = true
	fm.buffer.Close()
}

// @Override
func (fm *FloatOutputStream) RecordCheckpoint() {
	util.CheckState(!fm.closed)
	fm.checkpoints.Add(NewFloatStreamCheckpoint(fm.buffer.GetCheckpoint()))
}

// @Override
func (fm *FloatOutputStream) GetCheckpoints() *util.ArrayList[*FloatStreamCheckpoint] {
	util.CheckState(fm.closed)
	return fm.checkpoints
}

// @Override
func (fm *FloatOutputStream) GetStreamDataOutput(columnId metadata.MothColumnId) *StreamDataOutput {
	v, err := util.ToInt32Exact(fm.buffer.GetOutputDataSize())
	if err != nil {
		panic(err)
	}
	return NewStreamDataOutput2(fm.buffer.WriteDataTo, metadata.NewStream(columnId, metadata.DATA, v, false))
}

// @Override
func (fm *FloatOutputStream) GetBufferedBytes() int64 {
	return fm.buffer.EstimateOutputDataSize()
}

// @Override
func (fm *FloatOutputStream) GetRetainedBytes() int64 {
	return int64(FLOAT_OUT_INSTANCE_SIZE) + fm.buffer.GetRetainedSize()
}

// @Override
func (fm *FloatOutputStream) Reset() {
	fm.closed = false
	fm.buffer.Reset()
	fm.checkpoints.Clear()
}
