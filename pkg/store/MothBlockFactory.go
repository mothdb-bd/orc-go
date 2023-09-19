package store

import "github.com/mothdb-bd/orc-go/pkg/spi/block"

type MothBlockFactory struct {
	nestedLazy    bool
	currentPageId int32
}

func NewMothBlockFactory(nestedLazy bool) *MothBlockFactory {
	my := new(MothBlockFactory)
	my.nestedLazy = nestedLazy
	return my
}

func (my *MothBlockFactory) NextPage() {
	my.currentPageId++
}

func (my *MothBlockFactory) CreateBlock(positionCount int32, reader MothBlockReader, nested bool) block.Block {
	return block.NewLazyBlock(positionCount, NewMothBlockLoader(reader, nested && !my.nestedLazy))
}

type MothBlockReader interface {
	ReadBlock() block.Block
}

type MothBlockLoader struct {
	expectedPageId int32
	blockReader    MothBlockReader
	loadFully      bool
	loaded         bool
}

func NewMothBlockLoader(blockReader MothBlockReader, loadFully bool) *MothBlockLoader {
	mr := new(MothBlockLoader)
	mr.blockReader = blockReader
	mr.loadFully = loadFully
	return mr
}

// @Override
func (mr *MothBlockLoader) Load() block.Block {
	mr.loaded = true
	block := mr.blockReader.ReadBlock()
	if mr.loadFully {
		block = block.GetLoadedBlock()
	}
	return block
}
