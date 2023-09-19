package errors

import (
	"fmt"
	"strconv"
)

// 错误类型，ErrorType
type Kind int8

const (
	USER_ERROR             Kind = 0
	INTERNAL_ERROR         Kind = 1
	INSUFFICIENT_RESOURCES Kind = 2
	EXTERNAL               Kind = 3
)

// 错误编码
type Code int32

type StandardErrorCode struct {
	name string
	code Code
	kind Kind
}

func NewCode(name string, code Code, kind Kind) *StandardErrorCode {
	sec := &StandardErrorCode{}
	sec.name = name
	sec.code = code
	sec.kind = kind
	return sec
}

func (sec *StandardErrorCode) GetCode() Code {
	return sec.code
}

func (sec *StandardErrorCode) GetName() string {
	return sec.name
}

func (sec *StandardErrorCode) GetKind() Kind {
	return sec.kind
}

//@Override
func (sec *StandardErrorCode) ToString() string {
	return sec.name + ":" + strconv.FormatInt(int64(sec.code), 10)
}

var (

	// 内部错误
	// 通用内部错误
	GENERIC_INTERNAL_ERROR = NewCode("GENERIC_INTERNAL_ERROR", 65536, INTERNAL_ERROR)
)

type StandardError struct {
	error

	code *StandardErrorCode
	msg  string
}

func NewStandardError(code *StandardErrorCode, msg string) *StandardError {
	se := &StandardError{}
	se.code = code
	se.msg = msg

	return se
}

func (se *StandardError) GetCode() *StandardErrorCode {
	return se.code
}

// 实现接口
func (se *StandardError) Error() string {
	return fmt.Sprintf("Error code: %v , kind: %v, msg: %v", se.code.code, se.code.kind, se.msg)
}
