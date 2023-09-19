package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
)

type SliceColumnStatisticsBuilder interface {
	// 继承
	StatisticsBuilder

	// @Override
	// default void addBlock(Type type, block.Block block)
	// {
	//     for (int position = 0; position < block.GetPositionCount(); position++) {
	//         if (!block.IsNull(position)) {
	//             addValue(type.getSlice(block, position));
	//         }
	//     }
	// }
	AddBlock(kind block.Type, block block.Block)

	AddValue(value *slice.Slice)
}
