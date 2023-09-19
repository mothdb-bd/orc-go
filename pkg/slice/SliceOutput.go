package slice

import "github.com/mothdb-bd/orc-go/pkg/mothio"

// abstract
type SliceOutput interface {
	// 继承
	mothio.OutputStream
	// 继承
	mothio.DataOutput

	/**
	 * Resets this stream to the initial position.
	 */
	Reset()
	/**
	 * Resets this stream to the specified position.
	 */
	Reset2(position int32)

	/**
	 * Returns the {@code writerIndex} of this buffer.
	 */
	Size() int32

	/**
	 * Approximate number of bytes retained by this.
	 */
	GetRetainedSize() int64

	/**
	 * Returns the number of writable bytes which is equal to
	 * {@code (this.capacity - this.writerIndex)}.
	 */
	WritableBytes() int32
	/**
	 * Returns {@code true}
	 * if and only if {@code (this.capacity - this.writerIndex)} is greater
	 * than {@code 0}.
	 */
	IsWritable() bool

	//@Override
	WriteBoolean(value bool)

	//@Override
	WriteB(value byte) (n int, err error)
	/**
	 * Sets the specified byte at the current {@code writerIndex}
	 * and increases the {@code writerIndex} by {@code 1} in this buffer.
	 * The 24 high-order bits of the specified value are ignored.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 1}
	 */
	//  @Override
	WriteByte(value byte) error

	/**
	 * Sets the specified 16-bit short integer at the current
	 * {@code writerIndex} and increases the {@code writerIndex} by {@code 2}
	 * in this buffer.  The 16 high-order bits of the specified value are ignored.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 2}
	 */
	//  @Override
	WriteShort(value int16)

	/**
	 * Sets the specified 32-bit integer at the current {@code writerIndex}
	 * and increases the {@code writerIndex} by {@code 4} in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 4}
	 */
	//  @Override
	WriteInt(value int32)

	/**
	 * Sets the specified 64-bit long integer at the current
	 * {@code writerIndex} and increases the {@code writerIndex} by {@code 8}
	 * in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 8}
	 */
	//  @Override
	WriteLong(value int64)

	/**
	 * Sets the specified 32-bit float at the current
	 * {@code writerIndex} and increases the {@code writerIndex} by {@code 4}
	 * in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 4}
	 */
	//  @Override
	WriteFloat(v float32)

	/**
	 * Sets the specified 64-bit double at the current
	 * {@code writerIndex} and increases the {@code writerIndex} by {@code 8}
	 * in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.writableBytes} is less than {@code 8}
	 */
	//  @Override
	WriteDouble(value float64)

	/**
	 * Transfers the specified source buffer's data to this buffer starting at
	 * the current {@code writerIndex} until the source buffer becomes
	 * unreadable, and increases the {@code writerIndex} by the number of
	 * the transferred bytes.  This method is basically same with
	 * {@link #writeBytes(Slice, int, int)}, except that this method
	 * increases the {@code readerIndex} of the source buffer by the number of
	 * the transferred bytes while {@link #writeBytes(Slice, int, int)}
	 * does not.
	 *
	 * @throws IndexOutOfBoundsException if {@code source.readableBytes} is greater than {@code this.writableBytes}
	 */
	WriteSlice(source *Slice)

	WriteSlice2(source *Slice, sourceIndex int32, length int32)

	//@Override
	WriteBS(source []byte) (n int, err error)

	WriteBytes(source []byte)

	//@Override
	WriteBS2(source []byte, sourceIndex int32, length int32) (n int, err error)

	WriteBytes2(source []byte, sourceIndex int32, length int32)

	/**
	 * Transfers the content of the specified stream to this buffer
	 * starting at the current {@code writerIndex} and increases the
	 * {@code writerIndex} by the number of the transferred bytes.
	 *
	 * @param length the number of bytes to transfer
	 * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.writableBytes}
	 * @throws java.io.IOException if the specified stream threw an exception during I/O
	 */
	WriteInputStream(in mothio.InputStream, length int32)

	WriteZero(length int32)
	// {
	// 	if length == 0 {
	// 		return
	// 	}
	// 	if length < 0 {
	// 		panic("length must be 0 or greater than 0.")
	// 	}
	// 	nLong := maths.UnsignedRightShiftInt32(length, 3)
	// 	nBytes := length & 7
	// 	for i := nLong; i > 0; i-- {
	// 		st.WriteLong(0)
	// 	}
	// 	if nBytes == 4 {
	// 		st.WriteInt(0)
	// 	} else if nBytes < 4 {
	// 		for i := nBytes; i > 0; i-- {
	// 			st.WriteByte(0)
	// 		}
	// 	} else {
	// 		st.WriteInt(0)
	// 		for i := nBytes - 4; i > 0; i-- {
	// 			st.WriteByte(0)
	// 		}
	// 	}
	// }

	// abstract
	Slice() *Slice

	/**
	 * Returns the raw underlying slice of this output stream.  The slice may
	 * be larger than the size of this stream.
	 */
	GetUnderlyingSlice() *Slice

	/**
	 * Decodes this buffer's readable bytes into a string with the specified
	 * character set name.  This method is identical to
	 * {@code buf.toString(buf.readerIndex(), buf.readableBytes(), charsetName)}.
	 * This method does not modify {@code readerIndex} or {@code writerIndex} of
	 * this buffer.
	 */
	ToString() string

	AppendLong(value int64) SliceOutput

	AppendDouble(value float64) SliceOutput

	AppendInt(value int32) SliceOutput

	AppendShort(value int16) SliceOutput

	AppendByte(value byte) SliceOutput

	AppendBytes(source []byte, sourceIndex int32, length int32) SliceOutput

	AppendBytes2(source []byte) SliceOutput

	AppendSlice(slice *Slice) SliceOutput

	//@Override
	WriteChar(value byte)

	//@Override
	WriteChars(s string)

	//@Override
	WriteString(s string)
}
