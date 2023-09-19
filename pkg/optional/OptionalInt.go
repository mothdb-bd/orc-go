package optional

import (
	"fmt"
)

// OptionalEmpty() Optional --- return an empty Optional instance
// OptionalOf(value) Optional --- return an Optional with the specified present non-null value
// Optional.ToString() string --- return a non-empty string representation of this optional suitable for debugging
// Optional.Get() interface{} --- If a value is present in this Optional, returns the value
// Optional.IsPresent() bool --- Whether there is a value in Optional
// Optional.Equal(obj interface{}) bool --- Indicates whether some other object is "equal to" this Optional
type OptionalInt struct {
	data     int32
	hasValue bool
}

// How does go implement static method
// - no static method, use function instead
func OptionalIntEmpty() *OptionalInt {
	ot := new(OptionalInt)
	ot.hasValue = false
	return ot
}

func OptionalIntof(value int32) *OptionalInt {
	ot := new(OptionalInt)
	ot.data = value
	ot.hasValue = true
	return ot
}

func (op *OptionalInt) String() string {
	if op.hasValue {
		str := fmt.Sprintf("%v", op.data)
		return str
	} else {
		return "empty"
	}
}

func (op *OptionalInt) Get() int32 {
	return op.data
}

func (op *OptionalInt) IsPresent() bool {
	return op.hasValue
}

func (op *OptionalInt) Equals(value int32) bool {
	// Special case: value type is slice(map, struct)
	if op.hasValue {
		return value == op.data
	}
	return false
}

func (op *OptionalInt) OrElse(other int32) int32 {
	if op.hasValue {
		return op.data
	} else {
		return other
	}
}

// Could I use "[]" for custom type
// - maybe answer can be found in source code of map
