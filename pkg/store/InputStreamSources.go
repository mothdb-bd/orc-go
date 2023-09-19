package store

import (
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
)

// type InputStreamSources[T basic.Object] struct {
// 	// Map<StreamId, InputStreamSource<?>> streamSources;
// 	streamSources map[StreamId]InputStreamSource[T]
// }

// // public InputStreamSources(Map<StreamId, InputStreamSource<?>> streamSources)
// func NewInputStreamSources[T basic.Object](streamSources map[StreamId]InputStreamSource[T]) *InputStreamSources[T] {
// 	is := new(InputStreamSources[T])
// 	is.streamSources = streamSources
// 	return is
// }

// func (is *InputStreamSources[T]) GetInputStreamSource(column *MothColumn, streamKind metadata.StreamKind, streamType reflect.Type) InputStreamSource[T] {
// 	streamSource := is.streamSources[NewStreamId(column.GetColumnId(), streamKind)]
// 	if nil == streamSource {
// 		streamSource = MissingStreamSource[T](streamType)
// 	}
// 	return streamSource
// }

type InputStreamSources struct {
	// Map<StreamId, InputStreamSource<?>> streamSources;
	streamSources map[StreamId]InputStreamSource
}

// public InputStreamSources(Map<StreamId, InputStreamSource<?>> streamSources)
func NewInputStreamSources(streamSources map[StreamId]InputStreamSource) *InputStreamSources {
	is := new(InputStreamSources)
	is.streamSources = streamSources
	return is
}

func GetInputStreamSource[T IValueInputStream](is *InputStreamSources, column *MothColumn, streamKind metadata.StreamKind) InputStreamSource {
	streamSource := is.streamSources[NewStreamId(column.GetColumnId(), streamKind)]
	if nil == streamSource {
		streamSource = MissingStreamSource()
	}
	return streamSource
}
