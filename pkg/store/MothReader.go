package store

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	MAX_BATCH_SIZE           int32 = 8196
	INITIAL_BATCH_SIZE       int32 = 1
	BATCH_SIZE_GROWTH_FACTOR int32 = 2
	// mothReaderlog                      *Logger = Logger.get(MothReader.class)
	CURRENT_MAJOR_VERSION uint32 = 0
	CURRENT_MINOR_VERSION uint32 = 12
	EXPECTED_FOOTER_SIZE  int64  = 16 * 1024
)

type MothReader struct {
	mothDataSource    MothDataSource
	metadataReader    *metadata.ExceptionWrappingMetadataReader
	options           *MothReaderOptions
	hiveWriterVersion metadata.HiveWriterVersion
	bufferSize        int32
	compressionKind   metadata.CompressionKind
	decompressor      *optional.Optional[MothDecompressor]
	footer            *metadata.Footer
	metadata          *metadata.Metadata
	rootColumn        *MothColumn
}

func CreateMothReader(mothDataSource MothDataSource, options *MothReaderOptions) *optional.Optional[*MothReader] {
	mothDataSource = wrapWithCacheIfTiny(mothDataSource, options.GetTinyStripeThreshold())
	estimatedFileSize := mothDataSource.GetEstimatedSize()
	if estimatedFileSize > 0 && estimatedFileSize <= int64(len(metadata.MAGIC)) {
		panic(fmt.Sprintf("Invalid file size %d", estimatedFileSize))
	}
	expectedReadSize := maths.Min(estimatedFileSize, EXPECTED_FOOTER_SIZE)
	fileTail := mothDataSource.ReadTail(util.Int32Exact(expectedReadSize))
	if fileTail.Length() == 0 {
		return optional.Empty[*MothReader]()
	}
	return optional.Of(NewMothReader(mothDataSource, options, fileTail))
}
func NewMothReader(mothDataSource MothDataSource, options *MothReaderOptions, fileTail *slice.Slice) *MothReader {
	mr := new(MothReader)
	mr.options = options
	mr.mothDataSource = mothDataSource
	mr.metadataReader = metadata.NewExceptionWrappingMetadataReader(mothDataSource.GetId(), metadata.NewMothMetadataReader())
	postScriptSize, _ := fileTail.GetUInt8(fileTail.Size() - util.BYTE_BYTES)
	if int32(postScriptSize) >= fileTail.SizeInt32() {
		panic(fmt.Sprintf("Invalid postscript length %d", postScriptSize))
	}
	var postScript *metadata.PostScript
	s, _ := fileTail.MakeSlice(fileTail.Size()-util.BYTE_BYTES-int(postScriptSize), int(postScriptSize))
	postScript = mr.metadataReader.ReadPostScript(s.GetInput())
	checkMothVersion(mr.mothDataSource, postScript.GetVersion())
	mr.bufferSize = util.Int32ExactU(postScript.GetCompressionBlockSize())
	mr.compressionKind = postScript.GetCompression()
	mr.decompressor = CreateMothDecompressor(mothDataSource.GetId(), mr.compressionKind, mr.bufferSize)
	mr.hiveWriterVersion = postScript.GetHiveWriterVersion()
	footerSize := util.Int32Exact(postScript.GetFooterLength())
	metadataSize := util.Int32Exact(postScript.GetMetadataLength())
	var completeFooterSlice *slice.Slice
	completeFooterSize := footerSize + metadataSize + int32(postScriptSize) + util.BYTE_BYTES
	if completeFooterSize > fileTail.Length() {
		completeFooterSlice = mothDataSource.ReadTail(completeFooterSize)
	} else {
		completeFooterSlice, _ = fileTail.MakeSlice(int(fileTail.Length()-completeFooterSize), int(completeFooterSize))
	}
	metadataSlice, _ := completeFooterSlice.MakeSlice(0, int(metadataSize))
	metadataInputStream := NewMothInputStream(CreateChunkLoader(mothDataSource.GetId(), metadataSlice, mr.decompressor, memory.NewSimpleAggregatedMemoryContext()))
	mr.metadata = mr.metadataReader.ReadMetadata(mr.hiveWriterVersion, metadataInputStream)
	footerSlice, _ := completeFooterSlice.MakeSlice(int(metadataSize), int(footerSize))
	footerInputStream := NewMothInputStream(CreateChunkLoader(mothDataSource.GetId(), footerSlice, mr.decompressor, memory.NewSimpleAggregatedMemoryContext()))
	mr.footer = mr.metadataReader.ReadFooter(mr.hiveWriterVersion, footerInputStream)
	if mr.footer.GetTypes().Size() == 0 {
		panic("File has no columns")
	}
	mr.rootColumn = createMothColumn("", "", metadata.NewMothColumnId(0), mr.footer.GetTypes(), mothDataSource.GetId())
	return mr
}

func (mr *MothReader) GetColumnNames() *util.ArrayList[string] {
	return mr.footer.GetTypes().Get(metadata.ROOT_COLUMN).GetFieldNames()
}

func (mr *MothReader) GetFooter() *metadata.Footer {
	return mr.footer
}

func (mr *MothReader) GetMetadata() *metadata.Metadata {
	return mr.metadata
}

func (mr *MothReader) GetRootColumn() *MothColumn {
	return mr.rootColumn
}

func (mr *MothReader) GetBufferSize() int32 {
	return mr.bufferSize
}

func (mr *MothReader) GetCompressionKind() metadata.CompressionKind {
	return mr.compressionKind
}

func (mr *MothReader) CreateRecordReader(readColumns *util.ArrayList[*MothColumn], readTypes *util.ArrayList[block.Type], predicate MothPredicate, legacyFileTimeZone *time.Location, memoryUsage memory.AggregatedMemoryContext, initialBatchSize int32) *MothRecordReader {
	return mr.CreateRecordReader2(readColumns, readTypes, util.NCopysList(readColumns.Size(), FullyProjectedLayout()), predicate, 0, mr.mothDataSource.GetEstimatedSize(), legacyFileTimeZone, memoryUsage, initialBatchSize, NewFieldMapperFactory())
}

func (mr *MothReader) CreateRecordReader2(readColumns *util.ArrayList[*MothColumn], readTypes *util.ArrayList[block.Type], readLayouts util.List[ProjectedLayout], predicate MothPredicate, offset int64, length int64, legacyFileTimeZone *time.Location, memoryUsage memory.AggregatedMemoryContext, initialBatchSize int32, fieldMapperFactory FieldMapperFactory) *MothRecordReader {
	return NewMothRecordReader(readColumns, readTypes, readLayouts, predicate, int64(mr.footer.GetNumberOfRows()), mr.footer.GetStripes(), mr.footer.GetFileStats(), mr.metadata.GetStripeStatsList(), mr.mothDataSource, offset, length, mr.footer.GetTypes(), mr.decompressor, mr.footer.GetRowsInRowGroup(), legacyFileTimeZone, mr.hiveWriterVersion, mr.metadataReader, mr.options, mr.footer.GetUserMetadata(), memoryUsage, initialBatchSize, fieldMapperFactory)
}

func wrapWithCacheIfTiny(dataSource MothDataSource, maxCacheSize util.DataSize) MothDataSource {
	_, flag1 := dataSource.(*MemoryMothDataSource)
	_, flag2 := dataSource.(*CachingMothDataSource)

	if flag1 || flag2 {
		return dataSource
	}
	if dataSource.GetEstimatedSize() > int64(maxCacheSize.Bytes()) {
		return dataSource
	}
	data := dataSource.ReadTail(util.Int32Exact(dataSource.GetEstimatedSize()))
	dataSource.Close()
	return NewMemoryMothDataSource(dataSource.GetId(), data)
}

func createMothColumn(parentStreamName string, fieldName string, columnId metadata.MothColumnId, types *metadata.ColumnMetadata[*metadata.MothType], mothDataSourceId *common.MothDataSourceId) *MothColumn {
	path := util.Ternary(len(fieldName) == 0, parentStreamName, parentStreamName+"."+fieldName)
	mothType := types.Get(columnId)
	nestedColumns := util.NewArrayList[*MothColumn]()
	if mothType.GetMothTypeKind() == metadata.STRUCT {
		for fieldId := util.INT32_ZERO; fieldId < mothType.GetFieldCount(); fieldId++ {
			nestedColumns.Add(createMothColumn(path, mothType.GetFieldName(fieldId), mothType.GetFieldTypeIndex(fieldId), types, mothDataSourceId))
		}
	} else if mothType.GetMothTypeKind() == metadata.LIST {
		nestedColumns.Add(createMothColumn(path, "item", mothType.GetFieldTypeIndex(0), types, mothDataSourceId))
	} else if mothType.GetMothTypeKind() == metadata.MAP {
		nestedColumns.Add(createMothColumn(path, "key", mothType.GetFieldTypeIndex(0), types, mothDataSourceId), createMothColumn(path, "value", mothType.GetFieldTypeIndex(1), types, mothDataSourceId))
	} else if mothType.GetMothTypeKind() == metadata.UNION {
		for fieldId := util.INT32_ZERO; fieldId < mothType.GetFieldCount(); fieldId++ {
			nestedColumns.Add(createMothColumn(path, "field"+strconv.Itoa(int(fieldId)), mothType.GetFieldTypeIndex(fieldId), types, mothDataSourceId))
		}
	}
	return NewMothColumn(path, columnId, fieldName, mothType.GetMothTypeKind(), mothDataSourceId, nestedColumns, mothType.GetAttributes())
}

func checkMothVersion(mothDataSource MothDataSource, version []uint32) {
	l := len(version)
	if l >= 1 {
		major := version[0]
		minor := uint32(0)
		if l > 1 {
			minor = version[1]
		}
		if major > CURRENT_MAJOR_VERSION || (major == CURRENT_MAJOR_VERSION && minor > CURRENT_MINOR_VERSION) {
			log.Println(fmt.Sprintf("MOTH file %s was written by a newer Hive version %s. This file may not be readable by this version of Hive (%d.%d).", mothDataSource.String(), util.JoinNums(version, "."), CURRENT_MAJOR_VERSION, CURRENT_MINOR_VERSION))
		}
	}
}

func validateFile(input MothDataSource, readTypes *util.ArrayList[block.Type]) {
	mothReader := CreateMothReader(input, NewMothReaderOptions()).OrElseThrow("File is empty")
	mothRecordReader := mothReader.CreateRecordReader(mothReader.GetRootColumn().GetNestedColumns(), readTypes, TRUE, time.UTC, memory.NewSimpleAggregatedMemoryContext(), INITIAL_BATCH_SIZE)
	for page := mothRecordReader.NextPage(); page != nil; page = mothRecordReader.NextPage() {
		page.GetLoadedPage()
	}
}

type ProjectedLayout interface {
	GetFieldLayout(mothColumn *MothColumn) ProjectedLayout
}

type fullyProjectedLayout struct {
}

func (*fullyProjectedLayout) GetFieldLayout(mothColumn *MothColumn) ProjectedLayout {
	return new(fullyProjectedLayout)
}

func FullyProjectedLayout() ProjectedLayout {
	return new(fullyProjectedLayout)
}

type NameBasedProjectedLayout struct {
	// 继承
	ProjectedLayout

	fieldLayouts *optional.Optional[map[string]ProjectedLayout]
}

func NewNameBasedProjectedLayout(fieldLayouts *optional.Optional[map[string]ProjectedLayout]) *NameBasedProjectedLayout {
	nt := new(NameBasedProjectedLayout)
	nt.fieldLayouts = fieldLayouts
	return nt
}

// @Override
func (nt *NameBasedProjectedLayout) GetFieldLayout(mothColumn *MothColumn) ProjectedLayout {
	name := strings.ToLower(mothColumn.GetColumnName())
	if nt.fieldLayouts.IsPresent() {
		return nt.fieldLayouts.Get()[name]
	}
	return FullyProjectedLayout()
}

func CreateProjectedLayout(root *MothColumn, dereferences *util.ArrayList[*util.ArrayList[string]]) ProjectedLayout {
	if util.MapStream(dereferences.Stream(), (*util.ArrayList[string]).Size).AnyMatch(func(t int) bool {
		return t == 0
	}) {
		return FullyProjectedLayout()
	}
	var dereferencesByField map[string]*util.ArrayList[*util.ArrayList[string]]

	dereferences.ForEach(func(t *util.ArrayList[string]) {
		key := t.Get(0)
		re, ok := dereferencesByField[key]
		if ok {
			re.Add(t.SubList(1, t.Size()))
		} else {
			re = util.NewArrayList(t.SubList(1, t.Size()))
		}
		dereferencesByField[key] = re
	})

	// dereferencesByField := dereferences.Stream().collect(Collectors.groupingBy(func(sequence interface{}) {
	// 	sequence.get(0)
	// }, mapping(func(sequence interface{}) {
	// 	sequence.subList(1, sequence.size())
	// }, toList())))

	fieldLayouts := make(map[string]ProjectedLayout)
	for _, nestedColumn := range root.GetNestedColumns().ToArray() {
		fieldName := strings.ToLower(nestedColumn.GetColumnName())
		_, ok := dereferencesByField[fieldName]
		if ok {
			fieldLayouts[fieldName] = CreateProjectedLayout(nestedColumn, dereferencesByField[fieldName])
		}
	}
	return NewNameBasedProjectedLayout(optional.Of(fieldLayouts))
}

type FieldMapperFactory interface {
	Create(mothColumn *MothColumn) FieldMapper
}

type FieldMapper interface {
	Get(fieldName string) *MothColumn
}
