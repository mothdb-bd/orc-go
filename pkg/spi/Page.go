package spi

import (
	"fmt"
	"strconv"

	"github.com/mothdb-bd/orc-go/pkg/hashcode"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	PAGE_INSTANCE_SIZE int32         = util.SizeOf(&Page{})
	PAGE_EMPTY_BLOCKS  []block.Block = make([]block.Block, 0)
)

type Page struct {
	blocks              []block.Block
	positionCount       int32
	sizeInBytes         int64
	retainedSizeInBytes int64
	logicalSizeInBytes  int64
}

func wrapBlocksWithoutCopy(positionCount int32, blocks []block.Block) *Page {
	return NewPage4(false, positionCount, blocks)
}
func NewPage(blocks ...block.Block) *Page {
	return NewPage4(true, determinePositionCount(blocks), blocks)
}
func NewPage2(positionCount int32) *Page {
	return NewPage4(false, positionCount, PAGE_EMPTY_BLOCKS)
}
func NewPage3(positionCount int32, blocks ...block.Block) *Page {
	return NewPage4(true, positionCount, blocks)
}
func NewPage4(blocksCopyRequired bool, positionCount int32, blocks []block.Block) *Page {
	pe := new(Page)
	pe.positionCount = positionCount
	if blockSize(blocks) == 0 {
		pe.blocks = PAGE_EMPTY_BLOCKS
		pe.sizeInBytes = 0
		pe.logicalSizeInBytes = 0
		pe.retainedSizeInBytes = int64(PAGE_INSTANCE_SIZE)
	} else {
		if blocksCopyRequired {
			pe.blocks = blockClone(blocks)
		} else {
			pe.blocks = blocks
		}
	}
	return pe
}

func determinePositionCount(blocks []block.Block) int32 {
	if blockSize(blocks) == 0 {
		panic("blocks is empty")
	}
	return blocks[0].GetPositionCount()
}

/**
 * clone block
 */
func blockClone(blocks []block.Block) []block.Block {
	newBlocks := make([]block.Block, blockSize(blocks))
	for i := util.INT32_ZERO; i < blockSize(blocks); i++ {
		newBlocks[i] = blocks[i]
	}
	return newBlocks
}

/**
 * 计算block size
 */
func blockSize(blocks []block.Block) int32 {
	return int32(len(blocks))
}

func (pe *Page) GetChannelCount() int32 {
	return blockSize(pe.blocks)
}

func (pe *Page) GetPositionCount() int32 {
	return pe.positionCount
}

func (pe *Page) GetSizeInBytes() int64 {
	sizeInBytes := pe.sizeInBytes
	if sizeInBytes < 0 {
		sizeInBytes = 0
		for _, block := range pe.blocks {
			sizeInBytes += block.GetLoadedBlock().GetSizeInBytes()
		}
		pe.sizeInBytes = sizeInBytes
	}
	return sizeInBytes
}

func (pe *Page) GetLogicalSizeInBytes() int64 {
	logicalSizeInBytes := pe.logicalSizeInBytes
	if logicalSizeInBytes < 0 {
		logicalSizeInBytes = 0
		for _, block := range pe.blocks {
			logicalSizeInBytes += block.GetLogicalSizeInBytes()
		}
		pe.logicalSizeInBytes = logicalSizeInBytes
	}
	return logicalSizeInBytes
}

func (pe *Page) GetRetainedSizeInBytes() int64 {
	retainedSizeInBytes := pe.retainedSizeInBytes
	if retainedSizeInBytes < 0 {
		return pe.updateRetainedSize()
	}
	return retainedSizeInBytes
}

func (pe *Page) GetBlock(channel int32) block.Block {
	return pe.blocks[channel]
}

func (pe *Page) GetSingleValuePage(position int32) *Page {
	singleValueBlocks := make([]block.Block, blockSize(pe.blocks))
	for i := util.INT32_ZERO; i < blockSize(pe.blocks); i++ {
		singleValueBlocks[i] = pe.blocks[i].GetSingleValueBlock(position)
	}
	return wrapBlocksWithoutCopy(1, singleValueBlocks)
}

func (pe *Page) GetRegion(positionOffset int32, length int32) *Page {
	if positionOffset < 0 || length < 0 || positionOffset+length > pe.positionCount {
		panic(fmt.Sprintf("Invalid position %d and length %d in page with %d positions", positionOffset, length, pe.positionCount))
	}
	channelCount := pe.GetChannelCount()
	slicedBlocks := make([]block.Block, channelCount)
	for i := util.INT32_ZERO; i < channelCount; i++ {
		slicedBlocks[i] = pe.blocks[i].GetRegion(positionOffset, length)
	}
	return wrapBlocksWithoutCopy(length, slicedBlocks)
}

func (pe *Page) AppendColumn(block block.Block) *Page {
	if pe.positionCount != block.GetPositionCount() {
		panic("Block does not have same position count")
	}

	l := blockSize(pe.blocks)
	newBlocks := CopyOfBlocks(pe.blocks, l+1)
	newBlocks[l] = block
	return wrapBlocksWithoutCopy(pe.positionCount, newBlocks)
}

func CopyOfBlocks(original []block.Block, newLength int32) []block.Block {
	copy := make([]block.Block, newLength)
	ol := len(original)
	CopyBlocks(original, 0, copy, 0,
		maths.MinInt32(int32(ol), newLength))
	return copy
}

func CopyBlocks(from []block.Block, srcPos int32, dest []block.Block, destPos int, length int32) {
	n := 0
	for i := srcPos; i < length && int(i) < len(from); i++ {
		dest[n+destPos] = from[i]
		n++
	}
}

func (pe *Page) Compact() {
	if pe.GetRetainedSizeInBytes() <= pe.GetSizeInBytes() {
		return
	}
	for i := util.INT32_ZERO; i < blockSize(pe.blocks); i++ {
		newBlock := pe.blocks[i]
		_, flag := newBlock.(*block.DictionaryBlock)
		if flag {
			continue
		}
		pe.blocks[i] = newBlock.CopyRegion(0, newBlock.GetPositionCount())
	}
	dictionaryBlocks := pe.getRelatedDictionaryBlocks()
	for _, blockIndexes := range dictionaryBlocks {
		compactBlocks := compactRelatedBlocks(blockIndexes.GetBlocks())
		indexes := blockIndexes.GetIndexes()
		for i := 0; i < compactBlocks.Size(); i++ {
			pe.blocks[indexes.Get(i)] = compactBlocks.Get(i)
		}
	}
	pe.updateRetainedSize()
}

// Map<DictionaryId, DictionaryBlockIndexes>
func (pe *Page) getRelatedDictionaryBlocks() map[*block.DictionaryId]*DictionaryBlockIndexes {
	relatedDictionaryBlocks := make(map[*block.DictionaryId]*DictionaryBlockIndexes)
	for i := util.INT32_ZERO; i < blockSize(pe.blocks); i++ {
		newBlock := pe.blocks[i]
		dictionaryBlock, flag := newBlock.(*block.DictionaryBlock)
		if flag {
			// relatedDictionaryBlocks.computeIfAbsent(dictionaryBlock.getDictionarySourceId(), func(id interface{}) {
			// 	NewDictionaryBlockIndexes()
			// }).addBlock(dictionaryBlock, i)
			db := relatedDictionaryBlocks[dictionaryBlock.GetDictionarySourceId()]
			if db == nil {
				nDB := NewDictionaryBlockIndexes()
				db.AddBlock(dictionaryBlock, i)
				relatedDictionaryBlocks[dictionaryBlock.GetDictionarySourceId()] = nDB
			} else {
				db.AddBlock(dictionaryBlock, i)
			}
		}
	}
	return relatedDictionaryBlocks
}

// List<DictionaryBlock> blocks
func compactRelatedBlocks(blocks *util.ArrayList[*block.DictionaryBlock]) *util.ArrayList[*block.DictionaryBlock] {
	firstDictionaryBlock := blocks.Get(0)
	dictionary := firstDictionaryBlock.GetDictionary()
	positionCount := firstDictionaryBlock.GetPositionCount()
	dictionarySize := dictionary.GetPositionCount()
	dictionaryPositionsToCopy := make([]int32, maths.MinInt32(dictionarySize, positionCount))
	remapIndex := make([]int32, dictionarySize)
	util.FillInt32s(remapIndex, -1)
	numberOfIndexes := util.INT32_ZERO
	for i := util.INT32_ZERO; i < positionCount; i++ {
		position := firstDictionaryBlock.GetId(i)
		if remapIndex[position] == -1 {
			dictionaryPositionsToCopy[numberOfIndexes] = position
			remapIndex[position] = numberOfIndexes
			numberOfIndexes++
		}
	}
	if numberOfIndexes == dictionarySize {
		return blocks
	}
	newIds := getNewIds(positionCount, firstDictionaryBlock, remapIndex)
	outputDictionaryBlocks := util.NewArrayList[*block.DictionaryBlock]()
	newDictionaryId := block.RandomDictionaryId()

	for i := util.INT32_ZERO; i < positionCount; i++ {
		dictionaryBlock := blocks.GetByInt32(i)
		// for _, dictionaryBlock := range blocks {
		if firstDictionaryBlock.GetDictionarySourceId() != dictionaryBlock.GetDictionarySourceId() {
			panic("dictionarySourceIds must be the same")
		}
		compactDictionary := dictionaryBlock.GetDictionary().CopyPositions(dictionaryPositionsToCopy, 0, numberOfIndexes)
		// !(compactDictionary instanceof DictionaryBlock)
		_, flag := compactDictionary.(*block.DictionaryBlock)

		outputDictionaryBlocks.Add(block.NewDictionaryBlock5(positionCount, compactDictionary, newIds, !flag, newDictionaryId))
	}
	return outputDictionaryBlocks
}

func getNewIds(positionCount int32, dictionaryBlock *block.DictionaryBlock, remapIndex []int32) []int32 {
	newIds := make([]int32, positionCount)
	for i := util.INT32_ZERO; i < positionCount; i++ {
		newId := remapIndex[dictionaryBlock.GetId(i)]
		if newId == -1 {
			panic("reference to a non-existent key")
		}
		newIds[i] = newId
	}
	return newIds
}

func (pe *Page) GetLoadedPage() *Page {
	for i := util.INT32_ZERO; i < blockSize(pe.blocks); i++ {
		loaded := pe.blocks[i].GetLoadedBlock()
		if loaded != pe.blocks[i] {
			loadedBlocks := blockClone(pe.blocks)
			loadedBlocks[i] = loaded
			i++
			for ; i < blockSize(pe.blocks); i++ {
				loadedBlocks[i] = pe.blocks[i].GetLoadedBlock()
			}
			return wrapBlocksWithoutCopy(pe.positionCount, loadedBlocks)
		}
	}
	return pe
}

func (pe *Page) GetLoadedPage2(column int32) *Page {
	return wrapBlocksWithoutCopy(pe.positionCount, []block.Block{pe.blocks[column].GetLoadedBlock()})
}

func (pe *Page) GetLoadedPage3(columns ...int32) *Page {
	blocks := make([]block.Block, len(columns))
	for i := 0; i < len(columns); i++ {
		blocks[i] = pe.blocks[columns[i]].GetLoadedBlock()
	}
	return wrapBlocksWithoutCopy(pe.positionCount, blocks)
}

func (pe *Page) GetLoadedPage4(columns []int32, eagerlyLoadedColumns []int32) *Page {
	for _, column := range eagerlyLoadedColumns {
		pe.blocks[column] = pe.blocks[column].GetLoadedBlock()
	}
	if pe.retainedSizeInBytes != -1 && len(eagerlyLoadedColumns) > 0 {
		pe.updateRetainedSize()
	}
	blocks := make([]block.Block, len(columns))
	for i := 0; i < len(columns); i++ {
		blocks[i] = pe.blocks[columns[i]]
	}
	return wrapBlocksWithoutCopy(pe.positionCount, blocks)
}

// @Override
func (pe *Page) ToString() string {
	builder := util.NewSB().AppendString("Page{")
	builder.AppendString("positions=").AppendInt32(pe.positionCount)
	builder.AppendString(", channels=").AppendInt32(pe.GetChannelCount())
	builder.AppendInt8('}')
	builder.AppendString("@").AppendString(strconv.FormatInt(int64(hashcode.ObjectHashCode(pe)), 16))
	return builder.String()
}

func (pe *Page) GetPositions(retainedPositions []int32, offset int32, length int32) *Page {
	blocks := make([]block.Block, blockSize(pe.blocks))
	for i := util.INT32_ZERO; i < blockSize(blocks); i++ {
		blocks[i] = pe.blocks[i].GetPositions(retainedPositions, offset, length)
	}
	return wrapBlocksWithoutCopy(length, blocks)
}

func (pe *Page) CopyPositions(retainedPositions []int32, offset int32, length int32) *Page {
	blocks := make([]block.Block, blockSize(pe.blocks))
	for i := util.INT32_ZERO; i < blockSize(blocks); i++ {
		blocks[i] = pe.blocks[i].CopyPositions(retainedPositions, offset, length)
	}
	return wrapBlocksWithoutCopy(length, blocks)
}

func (pe *Page) GetColumns(column int32) *Page {
	return wrapBlocksWithoutCopy(pe.positionCount, []block.Block{pe.blocks[column]})
}

func (pe *Page) GetColumns2(columns ...int32) *Page {
	blocks := make([]block.Block, len(columns))
	for i := 0; i < len(columns); i++ {
		blocks[i] = pe.blocks[columns[i]]
	}
	return wrapBlocksWithoutCopy(pe.positionCount, blocks)
}

func (pe *Page) PrependColumn(column block.Block) *Page {
	if column.GetPositionCount() != pe.positionCount {
		panic(fmt.Sprintf("Column does not have same position count (%d) as page (%d)", column.GetPositionCount(), pe.positionCount))
	}
	result := make([]block.Block, blockSize(pe.blocks)+1)
	result[0] = column
	CopyBlocks(pe.blocks, 0, result, 1, blockSize(pe.blocks))
	return wrapBlocksWithoutCopy(pe.positionCount, result)
}

func (pe *Page) updateRetainedSize() int64 {
	retainedSizeInBytes := int64(PAGE_INSTANCE_SIZE + util.SizeOf(pe.blocks))
	for _, block := range pe.blocks {
		retainedSizeInBytes += block.GetRetainedSizeInBytes()
	}
	pe.retainedSizeInBytes = retainedSizeInBytes
	return retainedSizeInBytes
}

type DictionaryBlockIndexes struct {
	// 	private final *util.ArrayList[ ]<DictionaryBlock> blocks = new ArrayList<>();
	// 	private final *util.ArrayList[ ]<Integer> indexes = new ArrayList<>();
	blocks  *util.ArrayList[*block.DictionaryBlock]
	indexes *util.ArrayList[int32]
}

func NewDictionaryBlockIndexes() *DictionaryBlockIndexes {
	return new(DictionaryBlockIndexes)
}

func (ds *DictionaryBlockIndexes) AddBlock(block *block.DictionaryBlock, index int32) {
	ds.blocks.Add(block)
	ds.indexes.Add(index)
}

func (ds *DictionaryBlockIndexes) GetBlocks() *util.ArrayList[*block.DictionaryBlock] {
	return ds.blocks
}

func (ds *DictionaryBlockIndexes) GetIndexes() *util.ArrayList[int32] {
	return ds.indexes
}
