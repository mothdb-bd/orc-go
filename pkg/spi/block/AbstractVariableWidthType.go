package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/maths"
)

var EXPECTED_BYTES_PER_ENTRY int32 = 32

type AbstractVariableWidthType struct {
	VariableWidthType

	// 继承 AbstractType
	AbstractType
}

func NewAbstractVariableWidthType(signature *TypeSignature, goKind reflect.Kind) *AbstractVariableWidthType {
	ae := new(AbstractVariableWidthType)
	ae.signature = signature
	ae.goKind = goKind
	return ae
}

// @Override
func (ae *AbstractVariableWidthType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	expectedBytes := maths.MinInt32(expectedEntries*expectedBytesPerEntry, maxBlockSizeInBytes)

	var newexpectedEntries int32
	if expectedBytesPerEntry == 0 {
		newexpectedEntries = expectedEntries
	} else {
		newexpectedEntries = maths.MinInt32(expectedEntries, maxBlockSizeInBytes/expectedBytesPerEntry)
	}
	return NewVariableWidthBlockBuilder(blockBuilderStatus, newexpectedEntries, expectedBytes)
}

// @Override
func (ae *AbstractVariableWidthType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return ae.CreateBlockBuilder(blockBuilderStatus, expectedEntries, EXPECTED_BYTES_PER_ENTRY)
}
