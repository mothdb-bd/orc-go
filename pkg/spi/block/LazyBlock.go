package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type LazyBlock struct {

	// 继承
	Block

	positionCount int32
	lazyData      *LazyData
}

// var lazyBlockiNSTANCE_SIZE int32 = ClassLayout.parseClass(LazyBlock.class).instanceSize() + ClassLayout.parseClass(LazyData.class).instanceSize()

var LB_iNSTANCE_SIZE int32 = util.SizeOf(&LazyBlock{}) + util.SizeOf(&LazyBlock{})

func NewLazyBlock(positionCount int32, loader LazyBlockLoader) *LazyBlock {
	lk := new(LazyBlock)
	lk.positionCount = positionCount
	lk.lazyData = NewLazyData(positionCount, loader)
	return lk
}

// @Override
func (lk *LazyBlock) GetPositionCount() int32 {
	return lk.positionCount
}

// @Override
func (lk *LazyBlock) GetSliceLength(position int32) int32 {
	return lk.GetBlock().GetSliceLength(position)
}

// @Override
func (lk *LazyBlock) GetByte(position int32, offset int32) byte {
	return lk.GetBlock().GetByte(position, offset)
}

// @Override
func (lk *LazyBlock) GetShort(position int32, offset int32) int16 {
	return lk.GetBlock().GetShort(position, offset)
}

// @Override
func (lk *LazyBlock) GetInt(position int32, offset int32) int32 {
	return lk.GetBlock().GetInt(position, offset)
}

// @Override
func (lk *LazyBlock) GetLong(position int32, offset int32) int64 {
	return lk.GetBlock().GetLong(position, offset)
}

// @Override
func (lk *LazyBlock) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	return lk.GetBlock().GetSlice(position, offset, length)
}

// @Override
func (lk *LazyBlock) GetObject(position int32, clazz reflect.Type) basic.Object {
	return lk.GetBlock().GetObject(position, clazz)
}

// @Override
func (lk *LazyBlock) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	return lk.GetBlock().BytesEqual(position, offset, otherSlice, otherOffset, length)
}

// @Override
func (lk *LazyBlock) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	return lk.GetBlock().BytesCompare(position, offset, length, otherSlice, otherOffset, otherLength)
}

// @Override
func (lk *LazyBlock) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	lk.GetBlock().WriteBytesTo(position, offset, length, blockBuilder)
}

// @Override
func (lk *LazyBlock) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	return lk.GetBlock().Equals(position, offset, otherBlock, otherPosition, otherOffset, length)
}

// @Override
func (lk *LazyBlock) Hash(position int32, offset int32, length int32) int64 {
	return lk.GetBlock().Hash(position, offset, length)
}

// @Override
func (lk *LazyBlock) CompareTo(leftPosition int32, leftOffset int32, leftLength int32, rightBlock Block, rightPosition int32, rightOffset int32, rightLength int32) int32 {
	return lk.GetBlock().CompareTo(leftPosition, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength)
}

// @Override
func (lk *LazyBlock) GetSingleValueBlock(position int32) Block {
	return lk.GetBlock().GetSingleValueBlock(position)
}

// @Override
func (lk *LazyBlock) GetSizeInBytes() int64 {
	if !lk.IsLoaded() {
		return 0
	}
	return lk.GetBlock().GetSizeInBytes()
}

// @Override
func (lk *LazyBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	if !lk.IsLoaded() {
		return 0
	}
	return lk.GetBlock().GetRegionSizeInBytes(position, length)
}

// @Override
func (lk *LazyBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	if !lk.IsLoaded() {
		return 0
	}
	return lk.GetBlock().GetPositionsSizeInBytes(positions)
}

// @Override
func (lk *LazyBlock) GetRetainedSizeInBytes() int64 {
	if !lk.IsLoaded() {
		return int64(LB_iNSTANCE_SIZE)
	}
	return int64(LB_iNSTANCE_SIZE) + lk.GetBlock().GetRetainedSizeInBytes()
}

// @Override
func (lk *LazyBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	return lk.GetBlock().GetEstimatedDataSizeForStats(position)
}

// @Override
func (lk *LazyBlock) GetPositions(positions []int32, offset int32, length int32) Block {
	if lk.IsLoaded() {
		return lk.GetBlock().GetPositions(positions, offset, length)
	}
	checkArrayRange(positions, offset, length)
	return NewLazyBlock(length, NewPositionLazyBlockLoader(lk.lazyData, positions, offset, length))
}

// @Override
func (lk *LazyBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	return lk.GetBlock().CopyPositions(positions, offset, length)
}

// @Override
func (lk *LazyBlock) GetRegion(positionOffset int32, length int32) Block {
	if lk.IsLoaded() {
		return lk.GetBlock().GetRegion(positionOffset, length)
	}
	checkValidRegion(lk.GetPositionCount(), positionOffset, length)
	return NewLazyBlock(length, NewRegionLazyBlockLoader(lk.lazyData, positionOffset, length))
}

// @Override
func (lk *LazyBlock) CopyRegion(position int32, length int32) Block {
	return lk.GetBlock().CopyRegion(position, length)
}

// @Override
func (lk *LazyBlock) IsNull(position int32) bool {
	return lk.GetBlock().IsNull(position)
}

// @Override
func (lk *LazyBlock) MayHaveNull() bool {
	return lk.GetBlock().MayHaveNull()
}

// @Override
func (lk *LazyBlock) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList(lk.GetBlock())
}

func (lk *LazyBlock) GetBlock() Block {
	return lk.lazyData.GetTopLevelBlock()
}

// @Override
func (lk *LazyBlock) IsLoaded() bool {
	return lk.lazyData.IsFullyLoaded()
}

// @Override
func (lk *LazyBlock) GetLoadedBlock() Block {
	return lk.lazyData.GetFullyLoadedBlock()
}

func ListenForLoads(block Block, listener func(Block)) {
	addListenersRecursive(block, util.NewArrayList(listener))
}

type RegionLazyBlockLoader struct {
	delegate       *LazyData
	positionOffset int32
	length         int32
}

func NewRegionLazyBlockLoader(delegate *LazyData, positionOffset int32, length int32) *RegionLazyBlockLoader {
	rr := new(RegionLazyBlockLoader)
	rr.delegate = delegate
	rr.positionOffset = positionOffset
	rr.length = length
	return rr
}

// @Override
func (rr *RegionLazyBlockLoader) Load() Block {
	return rr.delegate.GetTopLevelBlock().GetRegion(rr.positionOffset, rr.length)
}

type PositionLazyBlockLoader struct {
	delegate  *LazyData
	positions []int32
	offset    int32
	length    int32
}

func NewPositionLazyBlockLoader(delegate *LazyData, positions []int32, offset int32, length int32) *PositionLazyBlockLoader {
	pr := new(PositionLazyBlockLoader)
	pr.delegate = delegate
	pr.positions = positions
	pr.offset = offset
	pr.length = length
	return pr
}

// @Override
func (pr *PositionLazyBlockLoader) Load() Block {
	return pr.delegate.GetTopLevelBlock().GetPositions(pr.positions, pr.offset, pr.length)
}

type LazyData struct {
	positionsCount int32           //@Nullable
	loader         LazyBlockLoader //@Nullable
	block          Block           //@Nullable
	listeners      *util.ArrayList[func(Block)]
}

func NewLazyData(positionsCount int32, loader LazyBlockLoader) *LazyData {
	la := new(LazyData)
	la.positionsCount = positionsCount
	la.loader = loader
	return la
}

func (la *LazyData) IsFullyLoaded() bool {
	return la.block != nil && la.block.IsLoaded()
}

func (la *LazyData) IsTopLevelBlockLoaded() bool {
	return la.block != nil
}

func (la *LazyData) GetTopLevelBlock() Block {
	la.load(false)
	return la.block
}

func (la *LazyData) GetFullyLoadedBlock() Block {
	if la.block != nil {
		return la.block.GetLoadedBlock()
	}
	la.load(true)
	return la.block
}

func (la *LazyData) addListeners(listeners *util.ArrayList[func(Block)]) {
	if la.IsTopLevelBlockLoaded() {
		panic("Top level block is already loaded")
	}
	if la.listeners == nil {
		la.listeners = util.NewArrayList[func(Block)]()
	}
	la.listeners.AddAll(listeners)
}

func (la *LazyData) load(recursive bool) {
	if la.loader == nil {
		return
	}
	la.block = la.loader.Load()
	if la.block.GetPositionCount() != la.positionsCount {
		panic(fmt.Sprintf("Loaded block positions count (%d) doesn't match lazy block positions count (%d)", la.block.GetPositionCount(), la.positionsCount))
	}
	if recursive {
		la.block = la.block.GetLoadedBlock()
	} else {
		lb, flag := la.block.(*LazyBlock)
		for flag {
			la.block = lb.GetBlock()

			lb, flag = la.block.(*LazyBlock)
		}
	}
	la.loader = nil
	listeners := la.listeners
	la.listeners = nil
	if listeners != nil {

		for i := 0; i < listeners.Size(); i++ {
			listeners.Get(i)(la.block)
		}

		if !recursive {
			addListenersRecursive(la.block, listeners)
		}
	}
}

// @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
func addListenersRecursive(block Block, listeners *util.ArrayList[func(Block)]) {

	lb, flag := block.(*LazyBlock)
	if flag {
		lazyData := lb.lazyData
		if !lazyData.IsTopLevelBlockLoaded() {
			lazyData.addListeners(listeners)
			return
		}
	}

	c := block.GetChildren()
	for i := 0; i < c.Size(); i++ {
		child := c.Get(i)
		addListenersRecursive(child, listeners)
	}

}
