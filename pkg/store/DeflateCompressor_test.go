package store

import "testing"

func TestDeflateCompressor_Compress(t *testing.T) {
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
		dr   Compressor
		args args
		want int32
	}{
		// TODO: Add test cases.
		{
			name: "Test DeflateCompressor",
			dr:   NewDeflateCompressor(),
			args: args{
				input:           []byte("1122cccdddeeaass908873331122"),
				inputOffset:     0,
				inputLength:     28,
				output:          make([]byte, 100),
				outputOffset:    0,
				maxOutputLength: 100,
			},
			want: 43,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.dr.Compress(tt.args.input, tt.args.inputOffset, tt.args.inputLength, tt.args.output, tt.args.outputOffset, tt.args.maxOutputLength); got != tt.want {
				t.Errorf("DeflateCompressor.Compress() = %v, want %v", got, tt.want)
			}
		})
	}
}
