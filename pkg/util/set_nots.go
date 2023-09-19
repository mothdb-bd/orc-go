package util

import (
	"fmt"
	"strings"

	"github.com/mothdb-bd/orc-go/pkg/basic"
)

// Provides a common set baseline for both threadsafe and non-ts Sets.
type set[T basic.ComObj] struct {
	// 继承
	SetInterface[T]
	m map[T]struct{} // struct{} doesn't take up space
}

// SetNonTS defines a non-thread safe set data structure.
type SetNonTS[T basic.ComObj] struct {
	set[T]
}

// NewNonTS creates and initializes a new non-threadsafe Set.
func newNonTS[T basic.ComObj]() *SetNonTS[T] {
	s := &SetNonTS[T]{}
	s.m = make(map[T]struct{})

	// Ensure interface compliance
	var _ SetInterface[T] = s

	return s
}

// Add includes the specified items (one or more) to the set. The underlying
// Set s is modified. If passed nothing it silently returns.
func (s *set[T]) Add(items ...T) {
	if len(items) == 0 {
		return
	}

	for _, item := range items {
		s.m[item] = keyExists
	}
}

// Remove deletes the specified items from the set.  The underlying Set s is
// modified. If passed nothing it silently returns.
func (s *set[T]) Remove(items ...T) {
	if len(items) == 0 {
		return
	}

	for _, item := range items {
		delete(s.m, item)
	}
}

// Pop  deletes and return an item from the set. The underlying Set s is
// modified. If set is empty, nil is returned.
func (s *set[T]) Pop() T {
	for item := range s.m {
		delete(s.m, item)
		return item
	}
	return basic.NullT[T]()
}

// Has looks for the existence of items passed. It returns false if nothing is
// passed. For multiple items it returns true only if all of  the items exist.
func (s *set[T]) Has(items ...T) bool {
	// assume checked for empty item, which not exist
	if len(items) == 0 {
		return false
	}

	has := true
	for _, item := range items {
		if _, has = s.m[item]; !has {
			break
		}
	}
	return has
}

// Size returns the number of items in a set.
func (s *set[T]) Size() int {
	return len(s.m)
}

// Clear removes all items from the set.
func (s *set[T]) Clear() {
	s.m = make(map[T]struct{})
}

// IsEmpty reports whether the Set is empty.
func (s *set[T]) IsEmpty() bool {
	return s.Size() == 0
}

// IsEqual test whether s and t are the same in size and have the same items.
func (s *set[T]) IsEqual(t SetInterface[T]) bool {
	// Force locking only if given set is threadsafe.
	if conv, ok := t.(*Set[T]); ok {
		conv.l.RLock()
		defer conv.l.RUnlock()
	}

	// return false if they are no the same size
	if sameSize := len(s.m) == t.Size(); !sameSize {
		return false
	}

	equal := true
	t.Each(func(item T) bool {
		_, equal = s.m[item]
		return equal // if false, Each() will end
	})

	return equal
}

// IsSubset tests whether t is a subset of s.
func (s *set[T]) IsSubset(t SetInterface[T]) (subset bool) {
	subset = true

	t.Each(func(item T) bool {
		_, subset = s.m[item]
		return subset
	})

	return
}

// IsSuperset tests whether t is a superset of s.
func (s *set[T]) IsSuperset(t SetInterface[T]) bool {
	return t.IsSubset(s)
}

// Each traverses the items in the Set, calling the provided function for each
// set member. Traversal will continue until all items in the Set have been
// visited, or if the closure returns false.
func (s *set[T]) Each(f func(item T) bool) {
	for item := range s.m {
		if !f(item) {
			break
		}
	}
}

func (s *set[T]) ForEach(f func(item T)) {
	for item := range s.m {
		f(item)
	}
}

func (s *set[T]) Stream() *Stream[T] {

	var sm *Stream[T]
	for item := range s.m {
		if sm == nil {
			sm = NewStream(item, NullStream[T]())
		} else {
			sm.Add(item)
		}
	}
	return sm
}

// Copy returns a new Set with a copy of s.
func (s *set[T]) Copy() SetInterface[T] {
	u := newNonTS[T]()
	for item := range s.m {
		u.Add(item)
	}
	return u
}

// String returns a string representation of s
func (s *set[T]) String() string {
	t := make([]string, 0, len(s.List()))
	for _, item := range s.List() {
		t = append(t, fmt.Sprintf("%v", item))
	}

	return fmt.Sprintf("[%s]", strings.Join(t, ", "))
}

// List returns a slice of all items. There is also StringSlice() and
// IntSlice() methods for returning slices of type string or int.
func (s *set[T]) List() []T {
	list := make([]T, 0, len(s.m))

	for item := range s.m {
		list = append(list, item)
	}

	return list
}

// Merge is like Union, however it modifies the current set it's applied on
// with the given t set.
func (s *set[T]) Merge(t SetInterface[T]) {
	t.Each(func(item T) bool {
		s.m[item] = keyExists
		return true
	})
}

// it's not the opposite of Merge.
// Separate removes the set items containing in t from set s. Please aware that
func (s *set[T]) Separate(t SetInterface[T]) {
	s.Remove(t.List()...)
}
