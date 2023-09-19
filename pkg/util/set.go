package util

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
)

// SetType denotes which type of set is created. ThreadSafe or NonThreadSafe
type SetType int

const (
	SET_ThreadSafe = iota
	SET_NonThreadSafe
)

func (s SetType) String() string {
	switch s {
	case SET_ThreadSafe:
		return "ThreadSafe"
	case SET_NonThreadSafe:
		return "NonThreadSafe"
	}
	return ""
}

// SetInterface is describing a Set. Sets are an unordered, unique list of values.
type SetInterface[T basic.ComObj] interface {
	Add(items ...T)
	Remove(items ...T)
	Pop() T
	Has(items ...T) bool
	Size() int
	Clear()
	IsEmpty() bool
	IsEqual(s SetInterface[T]) bool
	IsSubset(s SetInterface[T]) bool
	IsSuperset(s SetInterface[T]) bool
	Each(func(T) bool)
	String() string
	List() []T
	Copy() SetInterface[T]
	Merge(s SetInterface[T])
	Separate(s SetInterface[T])
	ForEach(f func(T))
	Stream() *Stream[T]
}

// helpful to not write everywhere struct{}{}
var keyExists = struct{}{}

func EmptySet[T basic.ComObj]() SetInterface[T] {
	return NewSet[T](SET_NonThreadSafe)
}

// New creates and initalizes a new Set interface. Its single parameter
// denotes the type of set to create. Either ThreadSafe or
// NonThreadSafe. The default is ThreadSafe.
func NewSet[T basic.ComObj](settype SetType) SetInterface[T] {
	if settype == SET_NonThreadSafe {
		return newNonTS[T]()
	}
	return newTS[T]()
}

func NewSetWithItems[T basic.ComObj](settype SetType, items ...T) SetInterface[T] {
	if settype == SET_NonThreadSafe {
		iter := newNonTS[T]()
		iter.Add(items...)
		return iter
	}
	iter := newTS[T]()
	iter.Add(items...)
	return iter
}

// Union is the merger of multiple sets. It returns a new set with all the
// elements present in all the sets that are passed.
//
// The dynamic type of the returned set is determined by the first passed set's
// implementation of the New() method.
func Union[T basic.ComObj](set1, set2 SetInterface[T], sets ...SetInterface[T]) SetInterface[T] {
	u := set1.Copy()
	set2.Each(func(item T) bool {
		u.Add(item)
		return true
	})
	for _, set := range sets {
		set.Each(func(item T) bool {
			u.Add(item)
			return true
		})
	}

	return u
}

// Difference returns a new set which contains items which are in in the first
// set but not in the others. Unlike the Difference() method you can use this
// function separately with multiple sets.
func Difference[T basic.ComObj](set1, set2 SetInterface[T], sets ...SetInterface[T]) SetInterface[T] {
	s := set1.Copy()
	s.Separate(set2)
	for _, set := range sets {
		s.Separate(set) // seperate is thread safe
	}
	return s
}

// Intersection returns a new set which contains items that only exist in all given sets.
func Intersection[T basic.ComObj](set1, set2 SetInterface[T], sets ...SetInterface[T]) SetInterface[T] {
	all := Union(set1, set2, sets...)
	result := Union(set1, set2, sets...)

	all.Each(func(item T) bool {
		if !set1.Has(item) || !set2.Has(item) {
			result.Remove(item)
		}

		for _, set := range sets {
			if !set.Has(item) {
				result.Remove(item)
			}
		}
		return true
	})
	return result
}

// SymmetricDifference returns a new set which s is the difference of items which are in
// one of either, but not in both.
func SymmetricDifference[T basic.ComObj](s SetInterface[T], t SetInterface[T]) SetInterface[T] {
	u := Difference(s, t)
	v := Difference(t, s)
	return Union(u, v)
}

// StringSlice is a helper function that returns a slice of strings of s. If
// the set contains mixed types of items only items of type string are returned.
func StringSlice(s SetInterface[string]) []string {
	slice := make([]string, 0)
	slice = append(slice, s.List()...)
	return slice
}

// IntSlice is a helper function that returns a slice of ints of s. If
// the set contains mixed types of items only items of type int are returned.
func IntSlice(s SetInterface[int]) []int {
	slice := make([]int, 0)
	slice = append(slice, s.List()...)
	return slice
}
