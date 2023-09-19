package util

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
)

// //泛型类型定义
// type T interface{}

// type U interface{}

//流计算数据结构定义

type Stream[T basic.Object] struct {
	Head     T
	Tail     *Stream[T]
	Length   int
	NotEmpty bool
}

func NullStream[T basic.Object]() *Stream[T] {
	return &Stream[T]{}
}

func GenStream[T basic.Object](r *Stream[T], f func(*Stream[T]) T, m int) *Stream[T] {
	if m == 1 {
		return r
	} else {
		return GenStream(NewStream(f(r), r), f, m-1)
	}
}

func NewStream[T basic.Object](head T, tail *Stream[T]) *Stream[T] {
	return &Stream[T]{head, tail, tail.Length + 1, true}
}

func (s *Stream[T]) Add(i T) *Stream[T] {
	return NewStream(i, s)
}

func (s *Stream[T]) Addall(i ...T) *Stream[T] {
	for _, v := range i {
		s = s.Add(v)
	}
	return s
}

// 左折叠 用于实现 reduce 的功能
func (s *Stream[T]) FoldLeft(i *Stream[T], f func(*Stream[T], T) *Stream[T]) *Stream[T] {
	if s.NotEmpty {
		return s.Tail.FoldLeft(f(i, s.Head), f)
	} else {
		return i
	}
}

// 右折叠
func (s *Stream[T]) FoldRight(i *Stream[T], f func(*Stream[T], T) *Stream[T]) *Stream[T] {
	if s.NotEmpty {
		return f(s.Tail.FoldRight(i, f), s.Head)
	} else {
		return i
	}
}

// 合并两个 Stream
func (s *Stream[T]) Merge(t *Stream[T]) *Stream[T] {
	if t.NotEmpty {
		return t.FoldRight(s, func(u *Stream[T], t T) *Stream[T] {
			return u.Add(t)
		})
	} else {
		return s
	}
}

// 倒序
func (s *Stream[T]) Reverse() *Stream[T] {
	return s.FoldLeft(NullStream[T](), func(u *Stream[T], t T) *Stream[T] {
		return u.Add(t)
	})
}

// 右折叠 用于map
func right[T basic.Object, R basic.Object](s *Stream[T], i *Stream[R], f func(*Stream[R], T) *Stream[R]) *Stream[R] {
	if s.NotEmpty {
		return f(right(s.Tail, i, f), s.Head)
	} else {
		return i
	}
}

// 右折叠 用于map
func rightForNumber[T basic.Object, R basic.Object](s *Stream[T], i R, f func(R, T) R) R {
	if s.NotEmpty {
		return f(rightForNumber(s.Tail, i, f), s.Head)
	} else {
		return i
	}
}

func (s *Stream[T]) MapToLong(f func(t T) int64) *LongStream {
	return rightForNumber(s, NullLong(), func(u *LongStream, t T) *LongStream {
		return u.Add(f(t))
	})
}

func (s *Stream[T]) MapToInt32(f func(t T) int32) *Int32Stream {
	return rightForNumber(s, NullInt32(), func(u *Int32Stream, t T) *Int32Stream {
		return u.Add(f(t))
	})
}

// MapStream
func MapStream[T basic.Object, R basic.Object](s *Stream[T], f func(t T) R) *Stream[R] {
	return right(s, NullStream[R](), func(u *Stream[R], t T) *Stream[R] {
		return u.Add(f(t))
	})
}

// Reduce
func (s *Stream[T]) Reduce(i T, f func(T, T) T) T {
	if s.NotEmpty {
		return s.Tail.Reduce(f(i, s.Head), f)
	} else {
		return i
	}
}

//过滤

func (s *Stream[T]) Filter(f func(T) bool) *Stream[T] {
	return s.FoldRight(NullStream[T](), func(u *Stream[T], t T) *Stream[T] {
		if f(t) {
			return u.Add(t)
		} else {
			return u
		}
	})
}

//AnyMatch

func (s *Stream[T]) AnyMatch(f func(T) bool) bool {
	if s.NotEmpty {
		re := f(s.Head)
		if re {
			return re
		} else {
			return re || s.Tail.AnyMatch(f)
		}
	}
	return false
}

//ForEach

func (s *Stream[T]) ForEach(f func(T)) {
	if s.NotEmpty {
		f(s.Head)
		s.Tail.ForEach(f)
	}
}

// To List
func (s *Stream[T]) ToList() *ArrayList[T] {
	list := NewArrayList[T]()
	s.ForEach(func(t T) {
		list.Add(t)
	})
	return list
}

//归并排序

func (s *Stream[T]) Sort(c func(T, T) bool) *Stream[T] {
	n := s.Length / 2
	if n == 0 {
		return s
	} else {
		x, y := split(s, NullStream[T](), n)
		return merge(x.Sort(c), y.Sort(c), c)
	}
}

func split[T basic.Object](x, y *Stream[T], n int) (*Stream[T], *Stream[T]) {
	if n == 0 || !x.NotEmpty {
		return x, y
	}
	return split(x.Tail, y.Add(x.Head), n-1)
}

func merge[T basic.Object](x, y *Stream[T], c func(T, T) bool) *Stream[T] {
	if !x.NotEmpty {
		return y
	}

	if !y.NotEmpty {
		return x
	}

	if c(x.Head, y.Head) {
		return merge(x.Tail, y, c).Add(x.Head)
	} else {
		return merge(x, y.Tail, c).Add(y.Head)
	}

}

// //格式化显示 Stream 的所有项

// func (s *Stream[T]) String() string {
// 	return "{" + strings.Join(s.FoldRight([]string{}, func(u U, t T) U {
// 		return append(u.([]string), fmt.Sprintf("%v", t))
// 	}).([]string), ",") + "}"
// }
