package block

import (
	atomic "sync/atomic"

	uuid "github.com/satori/go.uuid"
)

// var (
//
//	dictionaryIdnodeId		*UUID		= UUID.randomUUID()
//	dictionaryIdsequenceGenerator	*AtomicLong	= NewAtomicLong()
//
// )
var (
	nodeId, _        = uuid.NewV4()
	atomicLong int64 = 0
)

type DictionaryId struct {
	uuid       uuid.UUID
	sequenceId int64
}

func RandomDictionaryId() *DictionaryId {
	// return NewDictionaryId(nodeId.GetMostSignificantBits(), nodeId.getLeastSignificantBits(), sequenceGenerator.getAndIncrement())
	return NewDictionaryId(nodeId, atomic.AddInt64(&atomicLong, 1))
}
func NewDictionaryId(uuid uuid.UUID, sequenceId int64) *DictionaryId {
	dd := new(DictionaryId)
	dd.uuid = uuid
	dd.sequenceId = sequenceId
	return dd
}

func (dd *DictionaryId) GetUUID() uuid.UUID {
	return dd.uuid
}

func (dd *DictionaryId) GetSequenceId() int64 {
	return dd.sequenceId
}
