package errors

import "fmt"

type UnsupportedError struct {
	error

	Method string
}

// 初始化函数
func NewUnsupported(method string) *UnsupportedError {
	return &UnsupportedError{Method: method}
}

// 实现接口
func (f *UnsupportedError) Error() string {
	return fmt.Sprintf("当前方法 %v 尚不支持", f.Method)
}
