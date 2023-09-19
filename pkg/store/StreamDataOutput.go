package store

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type StreamDataOutput struct {
	// 继承
	MothDataOutput

	writer func(slice.SliceOutput) int64
	stream *metadata.Stream
}

func NewStreamDataOutput(s *slice.Slice, stream *metadata.Stream) *StreamDataOutput {
	return NewStreamDataOutput2(func(sliceOutput slice.SliceOutput) int64 {
		sliceOutput.WriteSlice2(s, 0, s.SizeInt32())
		return s.LenInt64()
	}, stream)
}
func NewStreamDataOutput2(writer func(slice.SliceOutput) int64, stream *metadata.Stream) *StreamDataOutput {
	st := new(StreamDataOutput)
	st.writer = writer
	st.stream = stream
	return st
}

// @Override
func (st *StreamDataOutput) CompareTo(otherStream *StreamDataOutput) int {
	return maths.Compare(st.Size(), otherStream.Size())
}

// @Override
func (st *StreamDataOutput) Size() int64 {
	return st.stream.LenInt64()
}

func (st *StreamDataOutput) GetStream() *metadata.Stream {
	return st.stream
}

// @Override
func (st *StreamDataOutput) WriteData(sliceOutput slice.SliceOutput) {
	size := st.writer(sliceOutput)
	util.Verify2(st.stream.LenInt64() == size, "Data stream did not write expected size")
}

// 继承
// util.Compare[*StreamDataOutput]
type StreamDataOutputCmp struct {
}

func NewStreamDataOutputCmp() *StreamDataOutputCmp {
	return new(StreamDataOutputCmp)
}

func (st StreamDataOutputCmp) Cmp(i, j *StreamDataOutput) int {
	return i.CompareTo(j)
}
