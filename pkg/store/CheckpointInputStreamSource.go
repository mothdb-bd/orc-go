package store

import (
	"github.com/mothdb-bd/orc-go/pkg/util"
)

//	type S interface {
//		~any
//	}
//
//	type C interface {
//		~any
//	}
//
// public class CheckpointInputStreamSource<S extends ValueInputStream<C>, C extends StreamCheckpoint> implements InputStreamSource<S>
type CheckpointInputStreamSource[C StreamCheckpoint] struct {
	// 继承
	InputStreamSource

	stream     IValueInputStream
	checkpoint C
}

func CreateCheckpointStreamSource[C StreamCheckpoint](stream IValueInputStream, checkpoint C) *CheckpointInputStreamSource[C] {
	return NewCheckpointInputStreamSource(stream, checkpoint)
}
func NewCheckpointInputStreamSource[C StreamCheckpoint](stream IValueInputStream, checkpoint C) *CheckpointInputStreamSource[C] {
	ce := new(CheckpointInputStreamSource[C])
	ce.stream = stream
	ce.checkpoint = checkpoint
	return ce
}

// @Nullable
// @Override
func (ce *CheckpointInputStreamSource[C]) OpenStream() IValueInputStream {
	ce.stream.(ValueInputStream[C]).SeekToCheckpoint(ce.checkpoint)
	return ce.stream
}

// @Override
func (ce *CheckpointInputStreamSource[C]) String() string {
	return util.NewSB().AddString("stream", ce.stream.String()).AddString("checkpoint", ce.checkpoint.String()).String()
}
