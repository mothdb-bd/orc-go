package metadata

import "github.com/mothdb-bd/orc-go/pkg/spi/block"

type LongValueStatisticsBuilder interface {
	//继承
	StatisticsBuilder

	// default void addBlock(Type type, block.Block block)
	// {
	//     for (int position = 0; position < block.GetPositionCount(); position++) {
	//         if (!block.IsNull(position)) {
	//             addValue(getValueFromBlock(type, block, position));
	//         }
	//     }
	// }
	AddBlock(kind block.Type, block block.Block)

	// default long getValueFromBlock(Type type, block.Block block, int position)
	// {
	//     return type.getLong(block, position);
	// }
	GetValueFromBlock(kind block.Type, block block.Block, position int32) int64
	AddValue(value int64)
}
