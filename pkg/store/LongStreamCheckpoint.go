package store

import (
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type LongStreamCheckpoint interface {
	// 继承
	StreamCheckpoint

	ToPositionList(compressed bool) *util.ArrayList[int32]
}
