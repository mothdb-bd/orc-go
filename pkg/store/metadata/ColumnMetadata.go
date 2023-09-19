package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ColumnMetadata[T basic.Object] struct {
	metadata *util.ArrayList[T]
}

func NewColumnMetadata[T basic.Object](metadata *util.ArrayList[T]) *ColumnMetadata[T] {
	ca := new(ColumnMetadata[T])
	ca.metadata = metadata
	return ca
}

func (ca *ColumnMetadata[T]) Get(columnId MothColumnId) T {
	return ca.metadata.Get(int(columnId))
}

func (ca *ColumnMetadata[T]) Size() int32 {
	return int32(ca.metadata.Size())
}

func (ca *ColumnMetadata[T]) List() *util.ArrayList[T] {
	return ca.metadata
}

func (ca *ColumnMetadata[T]) Stream() *util.Stream[T] {
	s := &util.Stream[T]{}

	for i := 0; i < int(ca.metadata.Size()); i++ {
		e := ca.metadata.Get(int(i))
		s.Add(e)
	}
	return s
}

func (ca *ColumnMetadata[T]) String() string {
	return "ColumnMetadata"
}
