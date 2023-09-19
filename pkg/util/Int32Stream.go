package util

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
)

//流计算数据结构定义

type Int32Stream struct {
	Head     int32
	Tail     *Int32Stream
	Length   int
	NotEmpty bool
}

func NullInt32() *Int32Stream {
	return &Int32Stream{}
}

func GenInt32Stream(r *Int32Stream, f func(*Int32Stream) int32, m int) *Int32Stream {
	if m == 1 {
		return r
	} else {
		return GenInt32Stream(NewInt32Stream(f(r), r), f, m-1)
	}
}

func NewInt32Stream(head int32, tail *Int32Stream) *Int32Stream {
	return &Int32Stream{head, tail, tail.Length + 1, true}
}

func (s *Int32Stream) Add(i int32) *Int32Stream {
	return NewInt32Stream(i, s)
}

func (s *Int32Stream) Addall(i ...int32) *Int32Stream {
	for _, v := range i {
		s = s.Add(v)
	}
	return s
}

// 左折叠 用于实现 reduce 的功能
func (s *Int32Stream) FoldLeft(i *Int32Stream, f func(*Int32Stream, int32) *Int32Stream) *Int32Stream {
	if s.NotEmpty {
		return s.Tail.FoldLeft(f(i, s.Head), f)
	} else {
		return i
	}
}

// 右折叠
func (s *Int32Stream) FoldRight(i *Int32Stream, f func(*Int32Stream, int32) *Int32Stream) *Int32Stream {
	if s.NotEmpty {
		return f(s.Tail.FoldRight(i, f), s.Head)
	} else {
		return i
	}
}

// 合并两个 Int32Stream
func (s *Int32Stream) Merge(t *Int32Stream) *Int32Stream {
	if t.NotEmpty {
		return t.FoldRight(s, func(u *Int32Stream, t int32) *Int32Stream {
			return u.Add(t)
		})
	} else {
		return s
	}
}

// 倒序
func (s *Int32Stream) Reverse() *Int32Stream {
	return s.FoldLeft(NullInt32(), func(u *Int32Stream, t int32) *Int32Stream {
		return u.Add(t)
	})
}

// 右折叠 用于map
func rightInt32[R basic.Object](s *Int32Stream, i *Stream[R], f func(*Stream[R], int32) *Stream[R]) *Stream[R] {
	if s.NotEmpty {
		return f(rightInt32(s.Tail, i, f), s.Head)
	} else {
		return i
	}
}

// Map
func MapInt32[R basic.Object](s *Int32Stream, f func(t int32) R) *Stream[R] {
	return rightInt32(s, NullStream[R](), func(u *Stream[R], t int32) *Stream[R] {
		return u.Add(f(t))
	})
}

//Reduce

func (s *Int32Stream) Reduce(i int32, f func(int32, int32) int32) int32 {
	if s.NotEmpty {
		return s.Tail.Reduce(f(i, s.Head), f)
	} else {
		return i
	}

}

//Sum

func (s *Int32Stream) Sum() int32 {
	if s.NotEmpty {
		return s.Head + s.Tail.Sum()
	} else {
		return 0
	}
}

//过滤

func (s *Int32Stream) Filter(f func(int32) bool) *Int32Stream {
	return s.FoldRight(NullInt32(), func(u *Int32Stream, t int32) *Int32Stream {
		if f(t) {
			return u.Add(t)
		} else {
			return u
		}
	})
}

//归并排序

func (s *Int32Stream) Sort(c func(int32, int32) bool) *Int32Stream {
	n := s.Length / 2
	if n == 0 {
		return s
	} else {
		x, y := splitInt32(s, NullInt32(), n)
		return mergeInt32(x.Sort(c), y.Sort(c), c)
	}
}

func splitInt32(x, y *Int32Stream, n int) (*Int32Stream, *Int32Stream) {
	if n == 0 || !x.NotEmpty {
		return x, y
	}
	return splitInt32(x.Tail, y.Add(x.Head), n-1)
}

func mergeInt32(x, y *Int32Stream, c func(int32, int32) bool) *Int32Stream {
	if !x.NotEmpty {
		return y
	}

	if !y.NotEmpty {
		return x
	}

	if c(x.Head, y.Head) {
		return mergeInt32(x.Tail, y, c).Add(x.Head)
	} else {
		return mergeInt32(x, y.Tail, c).Add(y.Head)
	}

}

// //格式化显示 Int32Stream 的所有项

// func (s *Int32Stream) String() string {
// 	return "{" + strings.Join(s.FoldRight([]string{}, func(u U, t int32) U {
// 		return append(u.([]string), fmt.Sprintf("%v", t))
// 	}).([]string), ",") + "}"
// }
