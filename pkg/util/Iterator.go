package util

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
)

type Iterator[T basic.Object] interface {
	HasNext() bool
	Next() T
	ForEach(func(T))
}

type Iterable[T basic.Object] interface {
	Iter() Iterator[T]

	ForEach(func(T))
}
