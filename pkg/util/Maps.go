package util

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
)

// basic map
var EMPTY_MAP map[string]string = make(map[string]string)

func PutAll[K basic.ComObj, V basic.Object](dest map[K]V, src map[K]V) {
	for k, v := range src {
		dest[k] = v
	}
}

func FilterKeys[K basic.ComObj, V basic.Object](src map[K]V, f func(K) bool) map[K]V {
	reMap := make(map[K]V)
	for k, v := range src {
		if f(k) {
			reMap[k] = v
		}
	}
	return reMap
}

func MapValues[K basic.ComObj, V basic.Object](src map[K]V) *ArrayList[V] {
	re := NewArrayList[V]()
	for _, v := range src {
		re.Add(v)
	}
	return re
}

func MapKeys[K basic.ComObj, V basic.Object](src map[K]V) *ArrayList[K] {
	re := NewArrayList[K]()
	for k := range src {
		re.Add(k)
	}
	return re
}

func UniqueIndex[K basic.ComObj, V basic.Object](values *ArrayList[V], keyFunction func(v V) K) map[K]V {
	CheckNotNull(keyFunction)
	reValues := make(map[K]V)
	for _, v := range values.ToArray() {
		reValues[keyFunction(v)] = v
	}
	return reValues
}

func EmptyMap[K basic.ComObj, V basic.Object]() map[K]V {
	return make(map[K]V)
}

/**
 * 给k,v 返回map
 */
func NewMap[K basic.ComObj, V basic.Object](k K, v V) map[K]V {
	return map[K]V{k: v}
}

// basic map End

// SetMap
type SetMap[K basic.ComObj, V basic.ComObj] struct {
	data map[K]SetInterface[V]
}

func NewSetMap[K basic.ComObj, V basic.ComObj]() *SetMap[K, V] {
	sm := new(SetMap[K, V])
	sm.data = make(map[K]SetInterface[V])
	return sm
}

/**
 *
 */
func (sm *SetMap[K, V]) Put(k K, v V) {
	s := sm.data[k]
	if s == nil {
		s = NewSetWithItems(SET_NonThreadSafe, v)
	} else {
		s.Add(v)
	}
	sm.data[k] = s
}

/**
 *
 */
func (sm *SetMap[K, V]) Get(k K) SetInterface[V] {
	return sm.data[k]
}

// SetMap End

// MothMap
type MothMap[K basic.ComObj, V basic.Object] map[K]V

func (m MothMap[K, V]) Put(k K, v V) {
	m[k] = v
}

// MothMap End
