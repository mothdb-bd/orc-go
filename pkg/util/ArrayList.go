package util

import (
	"sort"

	"github.com/mothdb-bd/orc-go/pkg/basic"
)

type Compare[T basic.Object] interface {
	Cmp(i, j T) int
}

type List[T basic.Object] interface {
	// 继承
	Iterable[T]

	Remove(index int) T
	Get(index int) T
	IsEmpty() bool
	Size() int

	AddIndex(index int32, values ...T) List[T]
	Add(values ...T) List[T]

	AddAll(values List[T]) List[T]
	AddAllIndex(index int32, values List[T]) List[T]

	Contains(value T) bool
	Clear()
	ForEach(f func(T))
	ToArray() []T

	Stream() *Stream[T]
	Iter() Iterator[T]
}

type CopiesList[T basic.Object] struct {
	// 继承
	List[T]

	n       int
	element T
}

func NCopysList[T basic.Object](n int, element T) List[T] {
	return &CopiesList[T]{n: n, element: element}
}

func (ct *CopiesList[T]) Remove(index int) T {
	panic("not implemented")
}

func (ct *CopiesList[T]) Get(index int) T {
	if index < 0 || index >= ct.n {
		panic("index out of range")
	}
	return ct.element
}
func (ct *CopiesList[T]) IsEmpty() bool {
	return ct.n == 0
}
func (ct *CopiesList[T]) Size() int {
	return ct.n
}

func (ct *CopiesList[T]) AddIndex(index int32, values ...T) List[T] {
	panic("not implemented")
}
func (ct *CopiesList[T]) Add(values ...T) List[T] {
	panic("not implemented")
}

func (ct *CopiesList[T]) AddAll(values List[T]) List[T] {
	panic("not implemented")
}
func (ct *CopiesList[T]) AddAllIndex(index int32, values List[T]) List[T] {
	panic("not implemented")
}

func (ct *CopiesList[T]) Contains(value T) bool {
	return ct.n != 0 && basic.ObjectEqual(ct.element, value)
}
func (ct *CopiesList[T]) Clear() {
	panic("not implemented")
}
func (ct *CopiesList[T]) ForEach(f func(T)) {
	for i := 0; i < ct.n; i++ {
		f(ct.element)
	}
}
func (ct *CopiesList[T]) ToArray() []T {
	if ct.n > 0 {
		re := make([]T, ct.n)
		for i := 0; i < ct.n; i++ {
			re[i] = ct.element
		}
		return re
	} else {
		return make([]T, 0)
	}
}

func (ct *CopiesList[T]) Stream() *Stream[T] {
	return GenStream(NewStream(ct.element, NullStream[T]()), func(s *Stream[T]) T {
		return ct.element
	}, ct.n)
}

func (ct *CopiesList[T]) Iter() Iterator[T] {
	return newCtIter(ct)
}

type ctIter[T basic.Object] struct {
	// 继承
	Iterator[T]

	index int
	ct    *CopiesList[T]
}

func newCtIter[T basic.Object](ct *CopiesList[T]) Iterator[T] {
	cr := new(ctIter[T])
	cr.ct = ct
	cr.index = 0
	return cr
}

func (it *ctIter[T]) Next() T {
	if it.HasNext() {
		it.index++
		return it.ct.element
	} else {
		panic("no more elements")
	}
}

func (it *ctIter[T]) HasNext() bool {
	return it.index < it.ct.n
}

func (it *ctIter[T]) ForEach(f func(t T)) {
	for it.HasNext() {
		f(it.Next())
	}
}

type ArrayList[T basic.Object] struct {
	// 继承
	List[T]

	// 继承
	sort.Interface

	elements []T
	size     int
	cmp      Compare[T]
}

// @Override for sort.Interface
func (list *ArrayList[T]) Len() int {
	return list.size
}

// @Override for sort.Interface
func (list *ArrayList[T]) Less(i, j int) bool {
	if list.cmp != nil {
		return list.cmp.Cmp(list.elements[i], list.elements[j]) < 0
	}
	panic("unsupported comparison")

}

// @Override for sort.Interface
func (list *ArrayList[T]) Swap(i, j int) {
	list.elements[i], list.elements[j] = list.elements[j], list.elements[i]
}

func EMPTY_LIST[T basic.Object]() *ArrayList[T] {
	return &ArrayList[T]{}
}

func NewArrayList[T basic.Object](values ...T) *ArrayList[T] {
	list := &ArrayList[T]{}
	list.elements = make([]T, 10)
	if len(values) > 0 {
		list.Add(values...)
	}

	return list
}

func NewCmpListWithValues[T basic.Object](cmp Compare[T], values ...T) *ArrayList[T] {
	list := &ArrayList[T]{}
	list.elements = make([]T, 10)
	if len(values) > 0 {
		list.Add(values...)
	}
	list.cmp = cmp
	return list
}

func NewCmpList[T basic.Object](cmp Compare[T]) *ArrayList[T] {
	list := &ArrayList[T]{}
	list.elements = make([]T, 10)
	list.cmp = cmp
	return list
}

func (list *ArrayList[T]) Add(values ...T) List[T] {
	if list.size+len(values) >= len(list.elements)-1 {
		newElements := make([]T, list.size+len(values)+1)
		copy(newElements, list.elements)
		list.elements = newElements
	}

	for _, value := range values {
		list.elements[list.size] = value
		list.size++
	}
	return list
}

func (list *ArrayList[T]) AddIndex(index int32, values ...T) List[T] {

	if list.size+len(values) >= len(list.elements)-1 {
		newElements := make([]T, list.size+len(values)+1)
		copy(newElements, list.elements)
		list.elements = newElements
	}

	numNew := Lens(values)
	numMoved := list.SizeInt32() - index
	if numMoved > 0 {
		CopyArrays(list.elements, index, list.elements, index+numNew, numMoved)
	}

	CopyArrays(values, 0, list.elements, index, numNew)
	list.size += int(numNew)
	return list
}

func (list *ArrayList[T]) AddAll(values List[T]) List[T] {
	list.Add(values.ToArray()...)
	return list
}

func (list *ArrayList[T]) AddAllIndex(index int32, values List[T]) List[T] {
	list.AddIndex(index, values.ToArray()...)
	return list
}

func (list *ArrayList[T]) Remove(index int) T {
	if index < 0 || index >= list.size {
		return basic.NullT[T]()
	}

	curEle := list.elements[index]
	list.elements[index] = basic.NullT[T]()
	copy(list.elements[index:], list.elements[index+1:list.size])
	list.size--
	return curEle
}

func (list *ArrayList[T]) Get(index int) T {
	if index < 0 || index >= list.size {
		return basic.NullT[T]()
	}
	return list.elements[index]
}

func (list *ArrayList[T]) GetByInt32(index int32) T {
	return list.Get(int(index))
}

func (list *ArrayList[T]) IsEmpty() bool {
	return list.size == 0
}

func (list *ArrayList[T]) Size() int {
	return list.size
}

func (list *ArrayList[T]) SizeInt32() int32 {
	return int32(list.size)
}
func (list *ArrayList[T]) Contains(value T) bool {
	for _, curValue := range list.elements {
		if basic.ObjectEqual(curValue, value) {
			return true
		}
	}

	return false
}

func (list *ArrayList[T]) Clear() {
	list.elements = nil
	list.elements = make([]T, 10)
	list.size = 0
}

func (list *ArrayList[T]) ForEach(f func(T)) {
	for i, t := range list.elements {
		if i < list.size {
			f(t)
		} else {
			break
		}
	}
}

func (list *ArrayList[T]) ToArray() []T {
	return list.elements[0:list.size:list.size]
}

func (list *ArrayList[T]) SubList(start int, end int) *ArrayList[T] {
	return NewArrayList(list.elements[start:end]...)
}

func (list *ArrayList[T]) Stream() *Stream[T] {
	if list.size == 0 {
		return NullStream[T]()
	} else {
		i := list.size - 1
		r := NewStream(list.elements[i], NullStream[T]())
		i--
		return GenStream(r, func(t *Stream[T]) T {
			e := list.elements[i]
			i--
			return e
		}, list.size)
	}
}

func (list *ArrayList[T]) Iter() Iterator[T] {
	return newAtIter(list)
}

func newAtIter[T basic.Object](list *ArrayList[T]) Iterator[T] {
	ar := new(atIter[T])
	ar.list = list
	ar.index = 0
	return ar
}

type atIter[T basic.Object] struct {
	// 继承
	Iterator[T]

	index int
	list  *ArrayList[T]
}

func (ar *atIter[T]) Next() T {
	if ar.HasNext() {
		t := ar.list.Get(ar.index)
		ar.index++
		return t
	} else {
		panic("no more elements")
	}
}

func (ar *atIter[T]) HasNext() bool {
	return ar.index < ar.list.Size()
}

func (ar *atIter[T]) ForEach(f func(t T)) {
	for ar.HasNext() {
		f(ar.Next())
	}
}

func ToArrayT[S basic.Object, T basic.Object](list *ArrayList[S]) []T {
	re := make([]T, list.Size())
	for i, s := range list.elements {
		if i < list.size {
			re[i] = (basic.Object(s).(T))
		} else {
			break
		}
	}
	return re
}
