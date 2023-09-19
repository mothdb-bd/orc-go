package store

import "testing"

func TestLz4Compressor_Compress(t *testing.T) {
	type args struct {
		input           []byte
		inputOffset     int32
		inputLength     int32
		output          []byte
		outputOffset    int32
		maxOutputLength int32
	}
	tests := []struct {
		name string
		sr   Compressor
		args args
		want int32
	}{
		// TODO: Add test cases.
		{
			name: "Test Lz4Compressor",
			sr:   NewLz4Compressor(),
			args: args{
				input:           []byte("1122cccdddeeaass908873331122"),
				inputOffset:     0,
				inputLength:     28,
				output:          make([]byte, 100),
				outputOffset:    0,
				maxOutputLength: 100,
			},
			want: 30,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sr.Compress(tt.args.input, tt.args.inputOffset, tt.args.inputLength, tt.args.output, tt.args.outputOffset, tt.args.maxOutputLength); got != tt.want {
				t.Errorf("Lz4Compressor.Compress() = %v, want %v", got, tt.want)
			}
		})
	}
}
