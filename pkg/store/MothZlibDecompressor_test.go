package store

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/mothdb-bd/orc-go/pkg/store/common"
)

func TestMothZlibDecompressor_Decompress(t *testing.T) {
	f, _ := os.Open("E:\\test\\mothdb\\zlib.gz")
	deBuffer, _ := ioutil.ReadAll(f)
	type args struct {
		input  []byte
		offset int32
		length int32
		output OutputBuffer
	}
	tests := []struct {
		name string
		mr   *MothZlibDecompressor
		args args
		want int32
	}{
		{
			// TODO: Add test cases.
			name: "test",
			mr:   NewMothZlibDecompressor(common.NewMothDataSourceId("id1"), 100),
			args: args{
				input:  deBuffer,
				offset: 0,
				length: 100,
				output: NewMemoryOutputBuffer(),
			},
			want: 28,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mr.Decompress(tt.args.input, tt.args.offset, tt.args.length, tt.args.output); got != tt.want {
				t.Errorf("MothZlibDecompressor.Decompress() = %v, want %v", got, tt.want)
			}
		})
	}
}

//	type OutputBuffer interface {
//		Initialize(size int32) []byte
//		Grow(size int32) []byte
//	}
type MemoryOutputBuffer struct {
	OutputBuffer
}

func NewMemoryOutputBuffer() OutputBuffer {
	return new(MemoryOutputBuffer)
}
func (buf *MemoryOutputBuffer) Initialize(size int32) []byte {
	return make([]byte, size)
}
func (buf *MemoryOutputBuffer) Grow(size int32) []byte {
	return make([]byte, size)
}
