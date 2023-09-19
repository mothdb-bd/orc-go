package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type StreamKind int8

const (
	PRESENT StreamKind = iota
	DATA
	LENGTH
	DICTIONARY_DATA
	DICTIONARY_COUNT
	SECONDARY
	ROW_INDEX
	BLOOM_FILTER
	BLOOM_FILTER_UTF8
)

type Stream struct {
	columnId   MothColumnId
	streamKind StreamKind
	length     int32
	useVInts   bool
}

func NewStream(columnId MothColumnId, streamKind StreamKind, length int32, useVInts bool) *Stream {
	sm := new(Stream)
	sm.columnId = columnId
	sm.streamKind = streamKind
	sm.length = length
	sm.useVInts = useVInts
	return sm
}

func (sm *Stream) GetColumnId() MothColumnId {
	return sm.columnId
}

func (sm *Stream) GetStreamKind() StreamKind {
	return sm.streamKind
}

func (sm *Stream) GetLength() int32 {
	return sm.length
}

func (sm *Stream) GetLengthPtr() *uint64 {
	ul := uint64(sm.length)
	return &ul
}

func (sm *Stream) LenInt64() int64 {
	return int64(sm.length)
}

func (sm *Stream) IsUseVInts() bool {
	return sm.useVInts
}

// @Override
func (sm *Stream) String() string {
	return util.NewSB().AddString("column", sm.columnId.String()).AddInt8("streamKind", int8(sm.streamKind)).AddInt32("length", sm.length).AddBool("useVInts", sm.useVInts).ToStringHelper()
}
