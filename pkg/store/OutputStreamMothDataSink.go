package store

import (
	"io"

	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var MOTH_DATA_SINK_INSTANCE_SIZE int32 = util.SizeOf(&OutputStreamMothDataSink{})

type OutputStreamMothDataSink struct {
	// 继承
	MothDataSink
	// 继承
	io.Closer

	// output *OutputStreamSliceOutput
	output *slice.OutputStreamSliceOutput
}

func NewOutputStreamMothDataSink(outputStream mothio.OutputStream) *OutputStreamMothDataSink {
	ok := new(OutputStreamMothDataSink)
	ok.output = slice.NewOutputStreamSliceOutput(outputStream)
	return ok
}

// @Override
func (ok *OutputStreamMothDataSink) Size() int64 {
	return ok.output.LongSize()
}

// @Override
func (ok *OutputStreamMothDataSink) GetRetainedSizeInBytes() int64 {
	return int64(MOTH_DATA_SINK_INSTANCE_SIZE) + ok.output.GetRetainedSize()
}

// @Override
func (ok *OutputStreamMothDataSink) Write(outputData *util.ArrayList[MothDataOutput]) {
	outputData.ForEach(func(data MothDataOutput) {
		data.WriteData(ok.output)
	})
}

// @Override
func (ok *OutputStreamMothDataSink) Close() {
	ok.output.Close()
}
