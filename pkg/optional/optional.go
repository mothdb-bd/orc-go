package optional

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

// OptionalEmpty() Optional --- return an empty Optional instance
// OptionalOf(value) Optional --- return an Optional with the specified present non-null value
// Optional.ToString() string --- return a non-empty string representation of this optional suitable for debugging
// Optional.Get() interface{} --- If a value is present in this Optional, returns the value
// Optional.IsPresent() bool --- Whether there is a value in Optional
// Optional.Equal(obj interface{}) bool --- Indicates whether some other object is "equal to" this Optional
type Optional[T basic.Object] struct {
	data      T
	hasValue  bool
	valueType string
}

// How does go implement static method
// - no static method, use function instead
func Empty[T basic.Object]() *Optional[T] {
	var optional *Optional[T] = new(Optional[T])
	// optional.data = basic.NullT[T]()
	optional.hasValue = false
	optional.valueType = ""
	return optional
}

func Of[T basic.Object](value T) *Optional[T] {
	var optional *Optional[T] = new(Optional[T])
	optional.data = value
	optional.hasValue = true
	optional.valueType = reflect.TypeOf(value).String()
	return optional
}

func (op *Optional[T]) String() string {
	if op.hasValue {
		str := fmt.Sprintf("%v", op.data)
		return str
	} else {
		return "empty"
	}
}

func (op *Optional[T]) Get() T {
	return op.data
}

func (op *Optional[T]) IsPresent() bool {
	return op.hasValue
}

func (op *Optional[T]) IsEmpty() bool {
	return !op.hasValue
}

func (op *Optional[T]) OrElse(other T) T {
	if op.hasValue {
		return op.data
	} else {
		return other
	}
}

func (op *Optional[T]) Equals(value T) bool {
	// Special case: value type is slice(map, struct)
	if op.hasValue {
		return basic.ObjectEqual(value, op.data)
	}
	return false
}

func OfNullable[T basic.Object](value T) *Optional[T] {
	return util.If(basic.ObjectEqual(value, nil), Empty[T](), Of(value)).(*Optional[T])
}

// Could I use "[]" for custom type
// - maybe answer can be found in source code of map
func Map[T basic.Object, R basic.Object](op *Optional[T], f func(T) R) *Optional[R] {
	if !op.IsPresent() {
		return Empty[R]()
	} else {
		return OfNullable(f(op.Get()))
	}
}

func (op *Optional[T]) IfPresent(f func(T)) {
	if op.IsPresent() {
		f(op.Get())
	}
}

func (op *Optional[T]) OrElseThrow(msg string) T {
	if op.hasValue {
		return op.data
	} else {
		panic(msg)
	}
}
