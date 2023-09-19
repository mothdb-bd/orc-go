package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/hashcode"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MapHashTables struct {
	mapType *MapType //@SuppressWarnings("VolatileArrayField")
	//@GuardedBy("this")
	//@Nullable
	hashTables []int32
}

// var (
// 	MapHashTables_INSTANCE_SIZE   int32 = ClassLayout.parseClass(MapHashTables.class).instanceSize()
// 	mapHashTableshASH_MULTIPLIER int32 = 2
// )

var (
	MHT_INSTANCE_SIZE         = util.SizeOf(&MapHashTables{})
	MHT_HASH_MULTIPLIER int32 = 2
)

// MapHashTables(MapType mapType, Optional<int[]> hashTables)
//
//	{
//		this.mapType = mapType;
//		this.hashTables = hashTables.orElse(null);
//	}
func NewMapHashTables(mapType *MapType, hashTables *optional.Optional[[]int32]) *MapHashTables {
	ms := new(MapHashTables)
	ms.mapType = mapType
	ms.hashTables = hashTables.OrElse(nil)
	return ms
}

func (ms *MapHashTables) GetRetainedSizeInBytes() int64 {
	return int64(MHT_INSTANCE_SIZE + util.SizeOf(ms.hashTables))
}

func (ms *MapHashTables) get() []int32 {
	if ms.hashTables == nil {
		panic("hashTables are not built")
	}
	return ms.hashTables
}

// Optional<int[]> tryGet()
//
//	{
//		return Optional.ofNullable(hashTables);
//	}
func (ms *MapHashTables) tryGet() *optional.Optional[[]int32] {
	return optional.Of(ms.hashTables)
}

func (ms *MapHashTables) growHashTables(newSize int32) {
	hashTables := ms.hashTables
	if hashTables == nil {
		panic("hashTables not set")
	}
	if newSize < util.Int32sLenInt32(hashTables) {
		panic("hashTables size does not match expectedEntryCount")
	}
	newRawHashTables := util.CopyOfInt32(hashTables, newSize)
	util.FillInt32Range(newRawHashTables, util.Int32sLenInt32(ms.hashTables), newSize, -1)
	ms.hashTables = newRawHashTables
}

func (ms *MapHashTables) buildAllHashTablesIfNecessary(rawKeyBlock Block, offsets []int32, mapIsNull []bool) {
	if ms.hashTables == nil {
		ms.buildAllHashTables(rawKeyBlock, offsets, mapIsNull)
	}
}

func (ms *MapHashTables) buildAllHashTables(rawKeyBlock Block, offsets []int32, mapIsNull []bool) {
	if ms.hashTables != nil {
		return
	}
	hashTables := make([]int32, rawKeyBlock.GetPositionCount()*MHT_HASH_MULTIPLIER)
	util.FillInt32s(hashTables, -1)
	hashTableCount := len(offsets) - 1
	for i := 0; i < hashTableCount; i++ {
		keyOffset := offsets[i]
		keyCount := offsets[i+1] - keyOffset
		if keyCount < 0 {
			panic(fmt.Sprintf("Offset is not monotonically ascending. offsets[%d]=%d, offsets[%d]=%d", i, offsets[i], i+1, offsets[i+1]))
		}
		if mapIsNull != nil && mapIsNull[i] && keyCount != 0 {
			panic("A null map must have zero entries")
		}
		ms.buildHashTableInternal(rawKeyBlock, keyOffset, keyCount, hashTables)
	}
	ms.hashTables = hashTables
}

func (ms *MapHashTables) buildHashTable(keyBlock Block, keyOffset int32, keyCount int32) {
	hashTables := ms.hashTables
	if hashTables == nil {
		panic("hashTables not set")
	}
	ms.buildHashTableInternal(keyBlock, keyOffset, keyCount, hashTables)
	ms.hashTables = hashTables
}

func (ms *MapHashTables) buildHashTableInternal(keyBlock Block, keyOffset int32, keyCount int32, hashTables []int32) {
	hashTableOffset := keyOffset * MHT_HASH_MULTIPLIER
	hashTableSize := keyCount * MHT_HASH_MULTIPLIER
	var i int32
	for i = 0; i < keyCount; i++ {
		hash := ms.getHashPosition(keyBlock, keyOffset+i, hashTableSize)
		for {
			if hashTables[hashTableOffset+hash] == -1 {
				hashTables[hashTableOffset+hash] = i
				break
			}
			hash++
			if hash == hashTableSize {
				hash = 0
			}
		}
	}
}

func (ms *MapHashTables) buildHashTableStrict(keyBlock Block, keyOffset int32, keyCount int32) {
	hashTables := ms.hashTables
	if hashTables == nil {
		panic("hashTables not set")
	}
	hashTableOffset := keyOffset * MHT_HASH_MULTIPLIER
	hashTableSize := keyCount * MHT_HASH_MULTIPLIER
	var i int32
	for i = 0; i < keyCount; i++ {
		hash := ms.getHashPosition(keyBlock, keyOffset+i, hashTableSize)
		for true {
			if hashTables[hashTableOffset+hash] == -1 {
				hashTables[hashTableOffset+hash] = i
				break
			}
			var isDuplicateKey bool
			obj := ReadNativeValue(ms.mapType.GetKeyType(), keyBlock, keyOffset+i)
			obj2 := ReadNativeValue(ms.mapType.GetKeyType(), keyBlock, keyOffset+hashTables[hashTableOffset+hash])
			if obj != nil && obj2 != nil {
				isDuplicateKey = basic.ObjectEqual(obj, obj2)
			} else {
				isDuplicateKey = false
			}
			if isDuplicateKey {
				panic("The map hash duplicate keys")
			}
			hash++
			if hash == hashTableSize {
				hash = 0
			}
		}
	}
	ms.hashTables = hashTables
}

func (ms *MapHashTables) getHashPosition(keyBlock Block, position int32, hashTableSize int32) int32 {
	if keyBlock.IsNull(position) {
		panic("map keys cannot be null")
	}
	var he int64
	obj := ReadNativeValue(ms.mapType.GetKeyType(), keyBlock, position)
	if obj != nil {
		he = int64(hashcode.ObjectHashCode(obj))
	} else {
		he = 0
	}
	return computePosition(he, hashTableSize)
}

func computePosition(he int64, hashTableSize int32) int32 {
	return int32(uint64(hashcode.Int64HashCode(he)*hashTableSize) >> 32)
}
