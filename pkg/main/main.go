package main

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/spi"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

func main() {
	// FileOutputStream out = new FileOutputStream(new File("E:\\test\\mothdb\\test.moth"));
	Delay(Write, 10)
	Delay(Read, 100)
}

func Read(num int) {
	// f, _ := os.Create("E:\\test\\mothdb\\test.moth")

	rOptions := store.NewMothReaderOptions()

	// fileDataSource := store.NewFileMothDataSource("E:\\test\\mothdb\\test.moth", rOptions)
	fileDataSource := store.NewFileMothDataSource("test.moth", rOptions)
	// List<Type>
	types := createTypes()

	mothReader := store.CreateMothReader(fileDataSource, rOptions)

	recordReader := mothReader.Get().CreateRecordReader(mothReader.Get().GetRootColumn().GetNestedColumns(), types, store.TRUE, time.UTC, memory.NewSimpleAggregatedMemoryContext(), store.INITIAL_BATCH_SIZE)

	fmt.Printf("GetFileRowCount %d\n", recordReader.GetFileRowCount())
	fmt.Printf("GetReaderRowCount %d\n", recordReader.GetReaderRowCount())

	page := recordReader.NextPage()

	re := make(map[int32]int)
	for page != nil {
		page = page.GetLoadedPage()
		chanelCount := page.GetChannelCount()
		for i := util.INT32_ZERO; i < chanelCount; i++ {
			b := page.GetBlock(i)
			kind := types.GetByInt32(i)
			for j := util.INT32_ZERO; j < b.GetPositionCount(); j++ {
				num, ok := re[i]
				if ok {
					nnum := num + 1
					re[i] = nnum
				} else {
					re[i] = 1
				}
				ae, ok := kind.(*block.ArrayType)
				if ok {
					obj := ae.GetObject(b, j)
					if obj != nil {
						fmt.Printf("Chanel is %d type is %s value is %s ,position is %d,pCount is %d\n", i, reflect.TypeOf(types.GetByInt32(i)), obj, j, b.GetPositionCount())
					}
				} else {
					obj := block.ReadNativeValue(kind, b, j)
					if obj != nil {
						fmt.Printf("Chanel is %d type is %s value is %s ,position is %d,pCount is %d\n", i, reflect.TypeOf(types.GetByInt32(i)), obj, j, b.GetPositionCount())
					}
				}
			}
		}
		page = recordReader.NextPage()
	}
	for i, j := range re {
		fmt.Printf("k,v: %d, %d \n", i, j)
	}
	recordReader.Close()
	mothReader.Get()
}

func createTypes() *util.ArrayList[block.Type] {
	// List<Type>
	types := util.NewArrayList[block.Type]()
	types.Add(block.NewArrayType(block.BIGINT))
	types.Add(block.BIGINT)
	types.Add(block.BOOLEAN)
	types.Add(block.CreateCharType(20))
	types.Add(block.DATE)
	types.Add(block.CreateDecimalType(10, 2))
	types.Add(block.CreateDecimalType(20, 2))
	types.Add(block.DOUBLE)
	types.Add(block.INTEGER)
	types.Add(block.TIMESTAMP_TZ_MILLIS)
	types.Add(block.TIMESTAMP)
	types.Add(block.TIMESTAMP_WITH_TIME_ZONE)
	// types.Add(block.NewMapType(block.INTEGER, block.CreateCharType(30)))
	// types.Add(block.NewQuantileDigestType(block.INTEGER))
	types.Add(block.REAL)
	// types.Add(block.Anonymous(util.NewArrayList[block.Type](block.BIGINT, block.CreateCharType(30))))
	types.Add(block.SMALLINT)
	types.Add(block.TINYINT)
	types.Add(block.VARBINARY)
	types.Add(block.CreateVarcharType(200))

	return types
}

func randomValues(position int, pb *spi.PageBuilder, types *util.ArrayList[block.Type]) {
	pb.DeclarePosition()

	chanel := util.INT32_ZERO
	// types.Add(block.NewArrayType(block.BIGINT))
	aType := types.GetByInt32(chanel)
	bb := pb.GetBlockBuilder(chanel)
	bigIntBB := block.BIGINT.CreateBlockBuilder2(nil, 10)
	bigIntBB.WriteLong(10).WriteLong(9)

	aType.WriteObject(bb, bigIntBB.Build())
	chanel++

	// types.Add(block.BIGINT)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), int64(10000000))
	chanel++
	// types.Add(block.BOOLEAN)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), true)
	chanel++
	// types.Add(block.CreateCharType(20))
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), "testchartype")
	chanel++
	// types.Add(block.DATE)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), int64(time.Now().Day()))
	chanel++
	// types.Add(block.CreateDecimalType(10, 2))
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), int64(100))
	chanel++
	// types.Add(block.CreateDecimalType(20, 2))
	s := slice.NewWithSize(16)
	s.WriteInt64LE(10)
	s.WriteInt64LE(18)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), s)
	chanel++
	// types.Add(block.DOUBLE)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), float64(8888.88))
	chanel++
	// types.Add(block.INTEGER)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), int64(9999999))
	chanel++
	// types.Add(block.TIMESTAMP_TZ_MILLIS)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), time.Now().UnixMilli())
	chanel++
	// types.Add(block.TIMESTAMP)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), time.Now().UnixMilli())
	chanel++
	// types.Add(block.TIMESTAMP_WITH_TIME_ZONE)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), time.Now().UnixMilli())
	chanel++
	// // types.Add(block.NewMapType(block.INTEGER, block.CreateCharType(30)))
	// block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), "1231214adfddde2dddddddddddddddd")
	// chanel++
	// types.Add(block.NewQuantileDigestType(block.INTEGER))
	// block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), 125487)
	// chanel++
	// types.Add(block.REAL)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), int64(199999))
	chanel++
	// types.Add(block.Anonymous(util.NewArrayList[block.Type](block.BIGINT, block.CreateCharType(30))))
	// block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), 12.5)
	// chanel++
	// types.Add(block.SMALLINT)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), int64(100))
	chanel++
	// types.Add(block.TINYINT)
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), int64(20))
	chanel++
	// types.Add(block.NewVarbinaryType())
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), []byte{255, 52})
	chanel++
	// types.Add(block.CreateVarcharType(200))
	block.WriteNativeValue(types.GetByInt32(chanel), pb.GetBlockBuilder(chanel), "25dafdad")
	chanel++
}

func createColumnNames(types *util.ArrayList[block.Type]) *util.ArrayList[string] {
	columnNames := util.NewArrayList[string]()
	for i := 0; i < types.Len(); i++ {
		columnNames.Add("f" + strconv.Itoa(i))
	}
	return columnNames
}

func Write(num int) {
	// f, _ := os.Create("E:\\test\\mothdb\\test.moth")
	f, _ := os.Create("test.moth")
	out := mothio.NewOutputStream(f)

	mothDataSink := store.NewOutputStreamMothDataSink(out)
	// List<String>
	types := createTypes()

	columnNames := createColumnNames(types)

	// types.Add(block.CreateVarcharType(20), block.CreateVarcharType(20), block.CreateVarcharType(20))
	// block.INTEGER, block.DOUBLE, block.CreateDecimalType(10, 2), , block.DOUBLE
	//ColumnMetadata<MothType>
	mothTypes := metadata.CreateRootMothType(columnNames, types)
	//MothWriterOptions

	options := store.Build().SetBloomFilterColumns(util.NewSetWithItems(util.SET_NonThreadSafe, "f1")).SetDictionaryMaxMemory(util.Ofds(256, util.MB)).Build()
	//  Map<String, String>
	userMetadata := util.EmptyMap[string, string]()
	// MothWriter
	writer := store.NewMothWriter(mothDataSink, columnNames, types, mothTypes, metadata.ZLIB, options, userMetadata, store.NewMothWriterStats())

	pb := spi.NewPageBuilder(types)
	for i := 0; i < num; i++ {
		randomValues(i, pb, types)
		if i%2000 == 0 {
			writer.Write(pb.Build())
			pb = spi.NewPageBuilder(types)
		}
	}
	// for i := 0; i < num; i++ {
	// 	block.WriteNativeValue(types.Get(6), pb.GetBlockBuilder(6), 9.2)
	// }

	writer.Write(pb.Build())

	writer.Close()
}

func Delay(f func(num int), num int) {
	start := time.Now()
	f(num)
	end := time.Now()
	fmt.Printf("耗时 : %d 毫秒\n", end.Sub(start).Milliseconds())
}
