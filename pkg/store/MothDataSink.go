package store

import "github.com/mothdb-bd/orc-go/pkg/util"

// MothDataSink
type MothDataSink interface {
	Size() int64
	GetRetainedSizeInBytes() int64
	Write(outputData *util.ArrayList[MothDataOutput])
	Close()
}
