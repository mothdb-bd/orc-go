package util

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
)

// 校验
type Predicate[T basic.Object] interface {
	Test(t T) bool
}

func CheckState(expression bool) {
	if !expression {
		panic("Illegal state")
	}
}

func CheckState2(expression bool, msg string) {
	if !expression {
		panic(msg)
	}
}

func Verify(expression bool) {
	if !expression {
		panic("Verify")
	}
}

func Verify2(expression bool, msg string) {
	if !expression {
		panic(msg)
	}
}

func CheckArgument(expression bool) {
	if !expression {
		panic("Illegal state")
	}
}

func CheckArgument2(expression bool, msg string) {
	if !expression {
		panic(msg)
	}
}

func CheckNotNull[T basic.Object](reference T) T {
	if &reference == nil {
		panic("Null reference")
	}
	return reference
}
