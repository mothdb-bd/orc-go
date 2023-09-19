package spi

import (
	"fmt"
	"math"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var DEFAULT_INITIAL_EXPECTED_ENTRIES int32 = 8

type PageBuilder struct {
	blockBuilders []block.BlockBuilder
	// *util.ArrayList[ ]<Type> types;
	types             *util.ArrayList[block.Type]
	pageBuilderStatus *block.PageBuilderStatus
	declaredPositions int32
}

func NewPageBuilder(types *util.ArrayList[block.Type]) *PageBuilder {
	return NewPageBuilder2(DEFAULT_INITIAL_EXPECTED_ENTRIES, types)
}
func NewPageBuilder2(initialExpectedEntries int32, types *util.ArrayList[block.Type]) *PageBuilder {
	return NewPageBuilder3(initialExpectedEntries, block.DEFAULT_MAX_PAGE_SIZE_IN_BYTES, types, optional.Empty[[]block.BlockBuilder]())
}

func WithMaxPageSize(maxPageBytes int32, types *util.ArrayList[block.Type]) *PageBuilder {
	return NewPageBuilder3(DEFAULT_INITIAL_EXPECTED_ENTRIES, maxPageBytes, types, optional.Empty[[]block.BlockBuilder]())
}

// PageBuilder(int initialExpectedEntries, int maxPageBytes, *util.ArrayList[ ]<? extends block.Type> types, *optional.Optional[ ]<BlockBuilder[]> templateBlockBuilders)
func NewPageBuilder3(initialExpectedEntries int32, maxPageBytes int32, types *util.ArrayList[block.Type], templateBlockBuilders *optional.Optional[[]block.BlockBuilder]) *PageBuilder {
	pr := new(PageBuilder)
	pr.types = types
	pr.pageBuilderStatus = block.NewPageBuilderStatus2(maxPageBytes)
	pr.blockBuilders = make([]block.BlockBuilder, types.Size())
	if templateBlockBuilders.IsPresent() {
		templates := templateBlockBuilders.Get()
		checkArgument(bbSize(templates) == types.SizeInt32(), "Size of templates and types should match")
		for i := util.INT32_ZERO; i < bbSize(pr.blockBuilders); i++ {
			pr.blockBuilders[i] = templates[i].NewBlockBuilderLike(pr.pageBuilderStatus.CreateBlockBuilderStatus())
		}
	} else {
		for i := util.INT32_ZERO; i < bbSize(pr.blockBuilders); i++ {
			pr.blockBuilders[i] = types.GetByInt32(i).CreateBlockBuilder2(pr.pageBuilderStatus.CreateBlockBuilderStatus(), initialExpectedEntries)
		}
	}
	return pr
}

/**
 * 计算block size
 */
func bbSize(bbs []block.BlockBuilder) int32 {
	return int32(len(bbs))
}

func (pr *PageBuilder) Reset() {
	if pr.IsEmpty() {
		return
	}
	pr.pageBuilderStatus = block.NewPageBuilderStatus2(pr.pageBuilderStatus.GetMaxPageSizeInBytes())
	pr.declaredPositions = 0
	for i := util.INT32_ZERO; i < bbSize(pr.blockBuilders); i++ {
		pr.blockBuilders[i] = pr.blockBuilders[i].NewBlockBuilderLike(pr.pageBuilderStatus.CreateBlockBuilderStatus())
	}
}

func (pr *PageBuilder) NewPageBuilderLike() *PageBuilder {
	return NewPageBuilder3(pr.declaredPositions, pr.pageBuilderStatus.GetMaxPageSizeInBytes(), pr.types, optional.Of(pr.blockBuilders))
}

func (pr *PageBuilder) GetBlockBuilder(channel int32) block.BlockBuilder {
	return pr.blockBuilders[channel]
}

func (pr *PageBuilder) GetType(channel int32) block.Type {
	return pr.types.GetByInt32(channel)
}

func (pr *PageBuilder) DeclarePosition() {
	pr.declaredPositions++
}

func (pr *PageBuilder) DeclarePositions(positions int32) {
	pr.declaredPositions += positions
}

func (pr *PageBuilder) IsFull() bool {
	return pr.declaredPositions == math.MaxInt32 || pr.pageBuilderStatus.IsFull()
}

func (pr *PageBuilder) IsEmpty() bool {
	return pr.declaredPositions == 0
}

func (pr *PageBuilder) GetPositionCount() int32 {
	return pr.declaredPositions
}

func (pr *PageBuilder) GetSizeInBytes() int64 {
	return pr.pageBuilderStatus.GetSizeInBytes()
}

func (pr *PageBuilder) GetRetainedSizeInBytes() int64 {
	retainedSizeInBytes := util.INT64_ZERO
	for _, blockBuilder := range pr.blockBuilders {
		retainedSizeInBytes += blockBuilder.GetRetainedSizeInBytes()
	}
	return retainedSizeInBytes
}

func (pr *PageBuilder) Build() *Page {
	size := bbSize(pr.blockBuilders)
	if size == 0 {
		return NewPage2(pr.declaredPositions)
	}
	blocks := make([]block.Block, size)
	for i := util.INT32_ZERO; i < size; i++ {
		blocks[i] = pr.blockBuilders[i].Build()
		if blocks[i].GetPositionCount() != pr.declaredPositions {
			panic(fmt.Sprintf("Declared positions (%d) does not match block name is %s at %d's number of entries (%d)", pr.declaredPositions, reflect.TypeOf(blocks[i]), i, blocks[i].GetPositionCount()))
		}
	}
	return wrapBlocksWithoutCopy(pr.declaredPositions, blocks)
}

func checkArgument(expression bool, errorMessage string) {
	if !expression {
		panic(errorMessage)
	}
}
