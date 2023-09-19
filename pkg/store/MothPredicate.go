package store

import (
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
)

var TRUE MothPredicate = &trueMothPredicate{}

type MothPredicate interface {
	Matches(numberOfRows int64, allColumnStatistics *metadata.ColumnMetadata[*metadata.ColumnStatistics]) bool
}

type trueMothPredicate struct {
	// 继承
	MothPredicate
}

func (*trueMothPredicate) Matches(numberOfRows int64, allColumnStatistics *metadata.ColumnMetadata[*metadata.ColumnStatistics]) bool {
	return true
}
