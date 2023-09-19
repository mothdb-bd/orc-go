package util

import "github.com/mothdb-bd/orc-go/pkg/basic"

func If(cond bool, a, b interface{}) interface{} {
	if cond {
		return a
	}

	return b
}

func Ternary[T basic.Object](cond bool, a, b T) T {
	if cond {
		return a
	}

	return b
}

func Try(f func(), catch func(interface{}), finally func()) {
	defer func() {
		if finally != nil {
			finally()
		}
	}()
	defer func() {
		if err := recover(); err != nil {
			if catch != nil {
				catch(err)
			} else {
				//loglogic.PFatal(err)//改成你自己的输出代码
				// do nothing
			}
		}
	}()
	f()
}
