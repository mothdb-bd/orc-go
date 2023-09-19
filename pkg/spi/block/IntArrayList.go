package block

import (
	"strconv"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var DEFAULT_INITIAL_CAPACITY int32 = 16

type IntArrayList struct {
	array []int32
	size  int32
}

func NewIntArrayList(initialCapacity int32) *IntArrayList {
	it := new(IntArrayList)
	if initialCapacity < 0 {
		// panic(NewIllegalArgumentException(format("Initial capacity '%s' is negative", initialCapacity)))
		panic("Initial capacity " + strconv.Itoa(int(initialCapacity)) + " is negative")
	}
	array := make([]int32, initialCapacity)
	it.array = array
	return it
}
func NewIntArrayList2() *IntArrayList {
	return NewIntArrayList(DEFAULT_INITIAL_CAPACITY)
}

func (it *IntArrayList) Elements() []int32 {
	return it.array
}

func (it *IntArrayList) grow(newCapacity int32) {
	if int32(len(it.array)) == MAX_ARRAY_SIZE {
		panic("Array reached maximum size")
	}
	if newCapacity > int32(len(it.array)) {
		max := maths.Max(int64(2*len(it.array)), int64(newCapacity))
		newLength := maths.Min(max, int64(MAX_ARRAY_SIZE))
		newArray := make([]int32, newLength)
		util.CopyInt32s(it.array, 0, newArray, 0, int32(newLength))
		it.array = newArray
	}
}

func (it *IntArrayList) Add(element int32) {
	it.grow(it.size + 1)
	it.array[it.size] = element
	it.size++
}

func (it *IntArrayList) Size() int32 {
	return it.size
}

func (it *IntArrayList) IsEmpty() bool {
	return it.size == 0
}
