package util

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
)

//流计算数据结构定义

type LongStream struct {
	Head     int64
	Tail     *LongStream
	Length   int
	NotEmpty bool
}

func NullLong() *LongStream {
	return &LongStream{}
}

func GenLongStream(r *LongStream, f func(*LongStream) int64, m int) *LongStream {
	if m == 1 {
		return r
	} else {
		return GenLongStream(NewLongStream(f(r), r), f, m-1)
	}
}

func NewLongStream(head int64, tail *LongStream) *LongStream {
	return &LongStream{head, tail, tail.Length + 1, true}
}

func (s *LongStream) Add(i int64) *LongStream {
	return NewLongStream(i, s)
}

func (s *LongStream) Addall(i ...int64) *LongStream {
	for _, v := range i {
		s = s.Add(v)
	}
	return s
}

// 左折叠 用于实现 reduce 的功能
func (s *LongStream) FoldLeft(i *LongStream, f func(*LongStream, int64) *LongStream) *LongStream {
	if s.NotEmpty {
		return s.Tail.FoldLeft(f(i, s.Head), f)
	} else {
		return i
	}
}

// 右折叠
func (s *LongStream) FoldRight(i *LongStream, f func(*LongStream, int64) *LongStream) *LongStream {
	if s.NotEmpty {
		return f(s.Tail.FoldRight(i, f), s.Head)
	} else {
		return i
	}
}

// 合并两个 LongStream
func (s *LongStream) Merge(t *LongStream) *LongStream {
	if t.NotEmpty {
		return t.FoldRight(s, func(u *LongStream, t int64) *LongStream {
			return u.Add(t)
		})
	} else {
		return s
	}
}

// 倒序
func (s *LongStream) Reverse() *LongStream {
	return s.FoldLeft(NullLong(), func(u *LongStream, t int64) *LongStream {
		return u.Add(t)
	})
}

// 右折叠 用于map
func rightLong[R basic.Object](s *LongStream, i *Stream[R], f func(*Stream[R], int64) *Stream[R]) *Stream[R] {
	if s.NotEmpty {
		return f(rightLong(s.Tail, i, f), s.Head)
	} else {
		return i
	}
}

// Map
func MapLong[R basic.Object](s *LongStream, f func(t int64) R) *Stream[R] {
	return rightLong(s, NullStream[R](), func(u *Stream[R], t int64) *Stream[R] {
		return u.Add(f(t))
	})
}

//Reduce

func (s *LongStream) Reduce(i int64, f func(int64, int64) int64) int64 {
	if s.NotEmpty {
		return s.Tail.Reduce(f(i, s.Head), f)
	} else {
		return i
	}

}

//Sum

func (s *LongStream) Sum() int64 {
	if s.NotEmpty {
		return s.Head + s.Tail.Sum()
	} else {
		return 0
	}
}

//过滤

func (s *LongStream) Filter(f func(int64) bool) *LongStream {
	return s.FoldRight(NullLong(), func(u *LongStream, t int64) *LongStream {
		if f(t) {
			return u.Add(t)
		} else {
			return u
		}
	})
}

//归并排序

func (s *LongStream) Sort(c func(int64, int64) bool) *LongStream {
	n := s.Length / 2
	if n == 0 {
		return s
	} else {
		x, y := splitLong(s, NullLong(), n)
		return mergeLong(x.Sort(c), y.Sort(c), c)
	}
}

func splitLong(x, y *LongStream, n int) (*LongStream, *LongStream) {
	if n == 0 || !x.NotEmpty {
		return x, y
	}
	return splitLong(x.Tail, y.Add(x.Head), n-1)
}

func mergeLong(x, y *LongStream, c func(int64, int64) bool) *LongStream {
	if !x.NotEmpty {
		return y
	}

	if !y.NotEmpty {
		return x
	}

	if c(x.Head, y.Head) {
		return mergeLong(x.Tail, y, c).Add(x.Head)
	} else {
		return mergeLong(x, y.Tail, c).Add(y.Head)
	}

}

// //格式化显示 LongStream 的所有项

// func (s *LongStream) String() string {
// 	return "{" + strings.Join(s.FoldRight([]string{}, func(u U, t int64) U {
// 		return append(u.([]string), fmt.Sprintf("%v", t))
// 	}).([]string), ",") + "}"
// }
