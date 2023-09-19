package store

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
)

func CreateDataOutput(s *slice.Slice) MothDataOutput {
	return newTmpMothDataOutput(s)
}

type MothDataOutput interface {
	Size() int64
	WriteData(sliceOutput slice.SliceOutput)
}

type tmpMothDataOutput struct {
	s *slice.Slice
}

func newTmpMothDataOutput(slice *slice.Slice) MothDataOutput {
	m := &tmpMothDataOutput{}
	m.s = slice
	return m
}

func (m *tmpMothDataOutput) Size() int64 {
	return int64(m.s.Size())
}

// func (m *tmpMothDataOutput) WriteData(sliceOutput *SliceOutput) {
func (m *tmpMothDataOutput) WriteData(sliceOutput slice.SliceOutput) {
	sliceOutput.WriteSlice2(m.s, 0, m.s.SizeInt32())
}
