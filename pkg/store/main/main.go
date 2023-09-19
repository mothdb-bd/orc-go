package main

import (
	"fmt"
	"math/rand"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
)

func main() {

	b := make([]byte, 256*1024*1024) //
	for i := 0; i < len(b); i++ {
		b[i] = byte(rand.Intn(255))
	}

	s := slice.NewWithBuf(b)

	// rOptions := store.NewMothReaderOptions()
	// fe := store.NewFileMothDataSource("E:\\test\\mothdb\\test.moth", rOptions)
	dataReader := store.NewMemoryMothDataReader(common.NewMothDataSourceId("1"), s, 0)

	memoryContext := memory.NewSimpleAggregatedMemoryContext()
	cl := store.NewUncompressedMothChunkLoader(dataReader, memoryContext)

	for cl.HasNextChunk() {
		tmpS := cl.NextChunk()
		fmt.Printf("Len : %d\n", tmpS.LenInt64())
	}

}
