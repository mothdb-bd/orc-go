package store

import (
	"io"
	"os"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type FileMothDataSource struct {
	// 继承
	AbstractMothDataSource

	input *mothio.RandomAccessFile
}

// *os.File
func NewFileMothDataSource(path string, options *MothReaderOptions) *FileMothDataSource {
	fe := new(FileMothDataSource)
	fe.id = common.NewMothDataSourceId(path)

	file, err := os.Open(path)
	if err != nil {
		panic(err.Error())
	}
	fe.input = mothio.NewRandomAccessFile(file)
	fi, _ := file.Stat()
	fe.estimatedSize = fi.Size()
	fe.options = options
	return fe
}

// @Override
func (fe *FileMothDataSource) Close() {
	fe.input.Close()
}

// @Override
func (fe *FileMothDataSource) readInternal(position int64, buffer []byte, bufferOffset int32, bufferLength int32) {
	fe.input.Seek(position, io.SeekStart)
	fe.input.ReadFully2(buffer, int(bufferOffset), int(bufferLength))
}

func (ae *FileMothDataSource) readTailInternal(length int32) *slice.Slice {
	return ae.ReadFully(ae.estimatedSize-int64(length), length)
}

// @Override
func (ae *FileMothDataSource) GetId() *common.MothDataSourceId {
	return ae.id
}

// @Override
func (ae *FileMothDataSource) GetReadBytes() int64 {
	return ae.readBytes
}

// @Override
func (ae *FileMothDataSource) GetReadTimeNanos() int64 {
	return ae.readTimeNanos
}

// @Override
func (ae *FileMothDataSource) GetEstimatedSize() int64 {
	return ae.estimatedSize
}

// @Override
func (ae *FileMothDataSource) ReadTail(length int32) *slice.Slice {
	start := time.Now()
	tailSlice := ae.readTailInternal(length)
	ae.readTimeNanos += time.Since(start).Nanoseconds()
	ae.readBytes += int64(tailSlice.Length())
	return tailSlice
}

// @Override
func (ae *FileMothDataSource) GetRetainedSize() int64 {
	return 0
}

// @Override
func (ae *FileMothDataSource) ReadFully(position int64, length int32) *slice.Slice {
	buffer := make([]byte, length)
	ae.readFully(position, buffer, 0, length)
	return slice.NewWithBuf(buffer)
}

func (ae *FileMothDataSource) readFully(position int64, buffer []byte, bufferOffset int32, bufferLength int32) {
	start := time.Now()
	ae.readInternal(position, buffer, bufferOffset, bufferLength)
	ae.readTimeNanos += time.Since(start).Nanoseconds()
	ae.readBytes += int64(bufferLength)
}

// @Override
func (ae *FileMothDataSource) ReadFully2(diskRanges map[StreamId]*DiskRange) map[StreamId]MothDataReader {
	if len(diskRanges) == 0 {
		return util.EmptyMap[StreamId, MothDataReader]()
	}
	maxReadSizeBytes := ae.options.GetMaxBufferSize().Bytes()
	smallRangesBuilder := make(map[StreamId]*DiskRange)
	largeRangesBuilder := make(map[StreamId]*DiskRange)
	for k, v := range diskRanges {
		if uint64(v.GetLength()) <= maxReadSizeBytes {
			smallRangesBuilder[k] = v
		} else {
			largeRangesBuilder[k] = v
		}
	}
	smallRanges := smallRangesBuilder
	largeRanges := largeRangesBuilder
	slices := make(map[StreamId]MothDataReader)
	util.PutAll(slices, ae.readSmallDiskRanges(smallRanges))
	util.PutAll(slices, ae.readLargeDiskRanges(largeRanges))
	return slices
}

func (ae *FileMothDataSource) readSmallDiskRanges(diskRanges map[StreamId]*DiskRange) map[StreamId]MothDataReader {
	if len(diskRanges) == 0 {
		return util.EmptyMap[StreamId, MothDataReader]()
	}
	mergedRanges := MergeAdjacentDiskRanges(util.MapValues(diskRanges), ae.options.GetMaxMergeDistance(), ae.options.GetMaxBufferSize())
	slices := make(map[StreamId]MothDataReader)
	if ae.options.IsLazyReadSmallRanges() {
		for _, mergedRange := range mergedRanges.ToArray() {
			mergedRangeLazyLoader := NewLazyBufferLoader(mergedRange, ae)
			for key, diskRange := range diskRanges {
				if mergedRange.Contains(diskRange) {
					slices[key] = NewMergedMothDataReader(ae.id, diskRange, mergedRangeLazyLoader)
				}
			}
		}
	} else {
		buffers := make(map[*DiskRange]*slice.Slice)
		for _, mergedRange := range mergedRanges.ToArray() {
			buffer := ae.ReadFully(mergedRange.GetOffset(), mergedRange.GetLength())
			buffers[mergedRange] = buffer
		}
		for k, v := range diskRanges {
			slices[k] = NewMemoryMothDataReader(ae.id, GetDiskRangeSlice(v, buffers), int64(v.GetLength()))
		}
	}
	// util.Verify(sliceStreams.keySet().equals(diskRanges.keySet()))
	return slices
}

func (ae *FileMothDataSource) readLargeDiskRanges(diskRanges map[StreamId]*DiskRange) map[StreamId]MothDataReader {
	if len(diskRanges) == 0 {
		return util.EmptyMap[StreamId, MothDataReader]()
	}
	slices := make(map[StreamId]MothDataReader)
	for k, v := range diskRanges {
		slices[k] = NewDiskMothDataReader(ae, v)
	}
	return slices
}

// @Override
func (ae *FileMothDataSource) String() string {
	return ae.id.String()
}

type DiskMothDataReader struct {
	// 继承
	AbstractDiskMothDataReader

	diskRange *DiskRange

	parent *FileMothDataSource
}

func NewDiskMothDataReader(parent *FileMothDataSource, diskRange *DiskRange) *DiskMothDataReader {
	dr := new(DiskMothDataReader)
	// NewdiskMothDataReader(id, requireNonNull(diskRange, "diskRange is null").getLength(), toIntExact(options.getStreamBufferSize().toBytes()))
	dr.parent = parent
	dr.mothDataSourceId = parent.GetId()
	dr.dataSize = diskRange.GetLength()
	dr.maxBufferSize = maths.MinInt32(int32(parent.options.GetStreamBufferSize().Bytes()), dr.dataSize)
	dr.diskRange = diskRange
	return dr
}

// @Override
func (dr *DiskMothDataReader) Read(position int64, buffer []byte, bufferOffset int32, length int32) {
	dr.parent.readFully(dr.diskRange.GetOffset()+position, buffer, bufferOffset, length)
}

// @Override
func (dr *DiskMothDataReader) String() string {
	return util.NewSB().AddString("mothDataSourceId", dr.GetMothDataSourceId().String()).AddString("diskRange", dr.diskRange.String()).AddInt32("maxBufferSize", dr.GetMaxBufferSize()).String()
}

// @Override
func (ar *DiskMothDataReader) GetMothDataSourceId() *common.MothDataSourceId {
	return ar.mothDataSourceId
}

// @Override
func (ar *DiskMothDataReader) GetRetainedSize() int64 {
	if ar.buffer == nil {
		return 0
	} else {
		return util.SizeOfInt64(ar.buffer)
	}
}

// @Override
func (ar *DiskMothDataReader) GetSize() int32 {
	return ar.dataSize
}

// @Override
func (ar *DiskMothDataReader) GetMaxBufferSize() int32 {
	return ar.maxBufferSize
}

// @Override
func (ar *DiskMothDataReader) SeekBuffer(newPosition int32) *slice.Slice {
	newBufferSize := maths.MinInt32(ar.dataSize-newPosition, ar.maxBufferSize)
	if ar.buffer == nil || util.Lens(ar.buffer) < newBufferSize {
		ar.buffer = make([]byte, newBufferSize)
	}
	if newPosition > ar.bufferStartPosition && newPosition < ar.bufferStartPosition+ar.bufferSize {
		overlapSize := (ar.bufferStartPosition + ar.bufferSize) - newPosition

		util.CopyBytes(ar.buffer, ar.bufferSize-overlapSize, ar.buffer, 0, overlapSize)
		// System.arraycopy()
		ar.Read(int64(newPosition+overlapSize), ar.buffer, overlapSize, newBufferSize-overlapSize)
	} else {
		ar.Read(int64(newPosition), ar.buffer, 0, newBufferSize)
	}
	ar.bufferSize = newBufferSize
	ar.bufferStartPosition = newPosition
	s := slice.NewWithBuf(ar.buffer)
	return s
}
