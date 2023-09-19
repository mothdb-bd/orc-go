package store

import "bytes"

type Decompressor interface {
	/**
	 * @return number of bytes written to the output
	 */
	Decompress(input []byte, inputOffset int32, inputLength int32, output []byte, outputOffset int32, maxOutputLength int32) int32

	// Decompress2(input ByteBuffer, output ByteBuffer)
	Decompress2(input bytes.Buffer, output bytes.Buffer)
}
