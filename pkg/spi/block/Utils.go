package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/basic"
)

type Utils struct {
}

func NativeValueToBlock(kind Type, object basic.Object) Block {
	// if object != nil {
	// 	expectedClass := Primitives.wrap(kind.GetGoType())
	// 	if !expectedClass.isInstance(object) {
	// 		panic(NewIllegalArgumentException(format("Object '%s' (%s) is not instance of %s", object, object.getClass().getName(), expectedClass.getName())))
	// 	}
	// }
	blockBuilder := kind.CreateBlockBuilder2(nil, 1)
	WriteNativeValue(kind, blockBuilder, object)
	return blockBuilder.Build()
}

func BlockToNativeValue(kind Type, block Block) basic.Object {
	if block.GetPositionCount() != 1 {
		panic(fmt.Sprintf("Block should have exactly one position, but has: %d", block.GetPositionCount()))
	}
	return ReadNativeValue(kind, block, 0)
}
