package store

import (
	"testing"

	"github.com/mothdb-bd/orc-go/pkg/store/common"
)

func TestMothSnappyDecompressor_Decompress(t *testing.T) {
	type args struct {
		input  []byte
		offset int32
		length int32
		output OutputBuffer
	}
	tests := []struct {
		name string
		mr   *MothSnappyDecompressor
		args args
		want int32
	}{
		// TODO: Add test cases.
		{
			// TODO: Add test cases.
			name: "test",
			mr:   NewMothSnappyDecompressor(common.NewMothDataSourceId("id1"), 100),
			args: args{
				input:  []byte("1122cccdddeeaass908873331122"),
				offset: 0,
				length: 28,
				output: NewMemoryOutputBuffer(),
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mr.Decompress(tt.args.input, tt.args.offset, tt.args.length, tt.args.output); got != tt.want {
				t.Errorf("MothSnappyDecompressor.Decompress() = %v, want %v", got, tt.want)
			}
		})
	}
}
