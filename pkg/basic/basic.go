package basic

import (
	"reflect"
	"strconv"
)

// 定义抽象类跟类
type Object interface{}

type Numbers interface {
	~byte | ~int8 | ~int16 | ~uint16 | ~int | ~uint | ~int32 | ~uint32 | ~int64 | ~uint64
}

type ComObj comparable

func NullT[T Object]() T {
	return *new(T)
}

/**
 * 比较两个objec是否相同
 */
func ObjectEqual(a, b Object) bool {
	return reflect.DeepEqual(a, b)
}

// 转换num为 string
func NumString[T Numbers](t T) string {
	return strconv.FormatInt(int64(t), 10)
}
