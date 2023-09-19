package store

import "github.com/mothdb-bd/orc-go/pkg/store/metadata"

type StreamId struct {
	columnId   metadata.MothColumnId
	streamKind metadata.StreamKind
}

func NewSId(stream *metadata.Stream) StreamId {
	sd := new(StreamId)
	sd.columnId = stream.GetColumnId()
	sd.streamKind = stream.GetStreamKind()
	return *sd
}
func NewStreamId(columnId metadata.MothColumnId, streamKind metadata.StreamKind) StreamId {
	sd := new(StreamId)
	sd.columnId = columnId
	sd.streamKind = streamKind
	return *sd
}

func (sd StreamId) GetColumnId() metadata.MothColumnId {
	return sd.columnId
}

func (sd StreamId) GetStreamKind() metadata.StreamKind {
	return sd.streamKind
}

func (sd StreamId) String() string {
	return sd.columnId.String()
}

func (sd StreamId) CompareTo(o StreamId) int {
	v := sd.columnId - o.columnId
	if v == 0 {
		k := sd.streamKind - o.streamKind
		if k > 0 {
			return 1
		} else if k < 0 {
			return -1
		} else {
			return 0
		}
	}
	return 0
}
