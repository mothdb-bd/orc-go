package util

import (
	"unsafe"

	"github.com/mothdb-bd/orc-go/pkg/basic"
)

func SizeOf(t basic.Object) int32 {
	return int32(unsafe.Sizeof(t))
}

func SizeOfInt64(t basic.Object) int64 {
	return int64(unsafe.Sizeof(t))
}
