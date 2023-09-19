package store

import (
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var PRESENT_INSTANCE_SIZE int32 = util.SizeOf(&PresentOutputStream{})

type PresentOutputStream struct {
	buffer              *MothOutputBuffer //@Nullable
	booleanOutputStream *BooleanOutputStream
	groupsCounts        *util.ArrayList[int32]
	currentGroupCount   int32
	closed              bool
}

func NewPresentOutputStream(compression metadata.CompressionKind, bufferSize int32) *PresentOutputStream {
	pm := new(PresentOutputStream)
	pm.buffer = NewMothOutputBuffer(compression, bufferSize)
	pm.groupsCounts = util.NewArrayList[int32]()
	return pm
}

func (pm *PresentOutputStream) WriteBoolean(value bool) {
	util.CheckArgument(!pm.closed)
	if !value && pm.booleanOutputStream == nil {
		pm.createBooleanOutputStream()
	}
	if pm.booleanOutputStream != nil {
		pm.booleanOutputStream.WriteBoolean(value)
	}
	pm.currentGroupCount++
}

func (pm *PresentOutputStream) createBooleanOutputStream() {
	util.CheckState(pm.booleanOutputStream == nil)
	pm.booleanOutputStream = NewBooleanOutputStream2(pm.buffer)
	for _, groupsCount := range pm.groupsCounts.ToArray() {
		pm.booleanOutputStream.writeBooleans(groupsCount, true)
		pm.booleanOutputStream.RecordCheckpoint()
	}
	pm.booleanOutputStream.writeBooleans(pm.currentGroupCount, true)
}

func (pm *PresentOutputStream) RecordCheckpoint() {
	util.CheckArgument(!pm.closed)
	pm.groupsCounts.Add(pm.currentGroupCount)
	pm.currentGroupCount = 0
	if pm.booleanOutputStream != nil {
		pm.booleanOutputStream.RecordCheckpoint()
	}
}

func (pm *PresentOutputStream) Close() {
	pm.closed = true
	if pm.booleanOutputStream != nil {
		pm.booleanOutputStream.Close()
	}
}

func (pm *PresentOutputStream) GetCheckpoints() *optional.Optional[*util.ArrayList[*BooleanStreamCheckpoint]] {
	util.CheckArgument(pm.closed)
	if pm.booleanOutputStream == nil {
		return optional.Empty[*util.ArrayList[*BooleanStreamCheckpoint]]()
	}
	return optional.Of(pm.booleanOutputStream.GetCheckpoints())
}

func (pm *PresentOutputStream) GetStreamDataOutput(columnId metadata.MothColumnId) *optional.Optional[*StreamDataOutput] {
	util.CheckArgument(pm.closed)
	if pm.booleanOutputStream == nil {
		return optional.Empty[*StreamDataOutput]()
	}
	streamDataOutput := pm.booleanOutputStream.GetStreamDataOutput(columnId)
	stream := metadata.NewStream(columnId, metadata.PRESENT, util.Int32Exact(streamDataOutput.Size()), streamDataOutput.GetStream().IsUseVInts())
	return optional.Of(NewStreamDataOutput2(func(sliceOutput slice.SliceOutput) int64 {
		streamDataOutput.WriteData(sliceOutput)
		return stream.LenInt64()
	}, stream))
}

func (pm *PresentOutputStream) GetBufferedBytes() int64 {
	if pm.booleanOutputStream == nil {
		return 0
	}
	return pm.booleanOutputStream.GetBufferedBytes()
}

func (pm *PresentOutputStream) GetRetainedBytes() int64 {
	if pm.booleanOutputStream == nil {
		return int64(PRESENT_INSTANCE_SIZE) + pm.buffer.GetRetainedSize()
	}
	return int64(PRESENT_INSTANCE_SIZE) + pm.booleanOutputStream.GetRetainedBytes()
}

func (pm *PresentOutputStream) Reset() {
	pm.closed = false
	pm.booleanOutputStream = nil
	pm.buffer.Reset()
	pm.groupsCounts.Clear()
	pm.currentGroupCount = 0
}
