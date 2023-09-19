package slice

import (
	"github.com/mothdb-bd/orc-go/pkg/mothio"
)

type SliceInput interface {
	// 继承
	mothio.InputStream
	// 继承
	mothio.DataInput

	/**
	 * Returns the {@code position} of this buffer.
	 */
	Position() int64

	/**
	 * Sets the {@code position} of this buffer.
	 *
	 * @throws IndexOutOfBoundsException if the specified {@code position} is
	 * less than {@code 0} or
	 * greater than {@code this.writerIndex}
	 */
	SetPosition(position int64)

	/**
	 * Returns {@code true}
	 * if and only if {@code available()} is greater
	 * than {@code 0}.
	 */
	IsReadable() bool

	/**
	 * Returns the number of bytes that can be read without blocking.
	 */
	//@Override
	Available() int32

	//@Override
	ReadBS() (byte, error)

	/**
	 * Returns true if the byte at the current {@code position} is not {@code 0} and increases
	 * the {@code position} by {@code 1} in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 1}
	 */
	//@Override
	ReadBoolean() bool

	/**
	 * Gets a byte at the current {@code position} and increases
	 * the {@code position} by {@code 1} in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 1}
	 */
	//@Override
	ReadByte() byte

	/**
	 * Gets an unsigned byte at the current {@code position} and increases
	 * the {@code position} by {@code 1} in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 1}
	 */
	//@Override
	ReadUnsignedByte() uint8

	/**
	 * Gets a 16-bit short integer at the current {@code position}
	 * and increases the {@code position} by {@code 2} in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 2}
	 */
	//@Override
	ReadShort() int16

	/**
	 * Gets an unsigned 16-bit short integer at the current {@code position}
	 * and increases the {@code position} by {@code 2} in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 2}
	 */
	//@Override
	ReadUnsignedShort() uint16

	/**
	 * Gets a 32-bit integer at the current {@code position}
	 * and increases the {@code position} by {@code 4} in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 4}
	 */
	//@Override
	ReadInt() int32

	/**
	 * Gets an unsigned 32-bit integer at the current {@code position}
	 * and increases the {@code position} by {@code 4} in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 4}
	 */
	ReadUnsignedInt() int64
	//  {
	// 	 return readInt() & 0xFFFFFFFFL;
	//  }

	/**
	 * Gets a 64-bit long at the current {@code position}
	 * and increases the {@code position} by {@code 8} in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 8}
	 */
	//@Override
	ReadLong() int64

	/**
	 * Gets a 32-bit float at the current {@code position}
	 * and increases the {@code position} by {@code 4} in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 4}
	 */
	//@Override
	ReadFloat() float32

	/**
	 * Gets a 64-bit double at the current {@code position}
	 * and increases the {@code position} by {@code 8} in this buffer.
	 *
	 * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 8}
	 */
	//@Override
	ReadDouble() float64

	/**
	 * Returns a new slice of this buffer's sub-region starting at the current
	 * {@code position} and increases the {@code position} by the size
	 * of the new slice (= {@code length}).
	 *
	 * @param length the size of the new slice
	 * @return the newly created slice
	 * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.available()}
	 */
	ReadSlice(length int32) *Slice

	//@Override
	ReadFully(destination []byte)
	//  {
	// 	 readBytes(destination);
	//  }

	//@Override
	ReadBS2(b []byte) (n int, err error)
	//  {
	// 	 return read(destination, 0, destination.length);
	//  }

	//@Override
	ReadBS3(b []byte, off int, l int) (n int, err error)
	/**
	 * Transfers this buffer's data to the specified destination starting at
	 * the current {@code position} and increases the {@code position}
	 * by the number of the transferred bytes (= {@code dst.length}).
	 *
	 * @throws IndexOutOfBoundsException if {@code dst.length} is greater than {@code this.available()}
	 */
	ReadBytes(destination []byte)
	//  {
	// 	 readBytes(destination, 0, destination.length);
	//  }

	//@Override
	ReadFully2(destination []byte, offset int, length int)
	//  {
	// 	 readBytes(destination, offset, length);
	//  }

	/**
	 * Transfers this buffer's data to the specified destination starting at
	 * the current {@code position} and increases the {@code position}
	 * by the number of the transferred bytes (= {@code length}).
	 *
	 * @param destinationIndex the first index of the destination
	 * @param length the number of bytes to transfer
	 * @throws IndexOutOfBoundsException if the specified {@code destinationIndex} is less than {@code 0},
	 * if {@code length} is greater than {@code this.available()}, or
	 * if {@code destinationIndex + length} is greater than {@code destination.length}
	 */
	ReadBytes2(destination []byte, destinationIndex int32, length int32)
	/**
	 * Transfers this buffer's data to the specified destination starting at
	 * the current {@code position} until the destination becomes
	 * non-writable, and increases the {@code position} by the number of the
	 * transferred bytes.  This method is basically same with
	 * {@link #readBytes(Slice, int, int)}, except that this method
	 * increases the {@code writerIndex} of the destination by the number of
	 * the transferred bytes while {@link #readBytes(Slice, int, int)}
	 * does not.
	 *
	 * @throws IndexOutOfBoundsException if {@code destination.writableBytes} is greater than
	 * {@code this.available()}
	 */
	ReadSlice2(destination *Slice)
	//  {
	// 	 readBytes(destination, 0, destination.length());
	//  }

	/**
	 * Transfers this buffer's data to the specified destination starting at
	 * the current {@code position} and increases the {@code position}
	 * by the number of the transferred bytes (= {@code length}).  This method
	 * is basically same with {@link #readBytes(Slice, int, int)},
	 * except that this method increases the {@code writerIndex} of the
	 * destination by the number of the transferred bytes (= {@code length})
	 * while {@link #readBytes(Slice, int, int)} does not.
	 *
	 * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.available()} or
	 * if {@code length} is greater than {@code destination.writableBytes}
	 */
	ReadSlice3(destination *Slice, length int32)
	//  {
	// 	 readBytes(destination, 0, length);
	//  }

	/**
	 * Transfers this buffer's data to the specified destination starting at
	 * the current {@code position} and increases the {@code position}
	 * by the number of the transferred bytes (= {@code length}).
	 *
	 * @param destinationIndex the first index of the destination
	 * @param length the number of bytes to transfer
	 * @throws IndexOutOfBoundsException if the specified {@code destinationIndex} is less than {@code 0},
	 * if {@code length} is greater than {@code this.available()}, or
	 * if {@code destinationIndex + length} is greater than
	 * {@code destination.capacity}
	 */
	ReadSlice4(destination *Slice, destinationIndex int32, length int32)

	/**
	 * Transfers this buffer's data to the specified stream starting at the
	 * current {@code position}.
	 *
	 * @param length the number of bytes to transfer
	 * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.available()}
	 * @throws java.io.IOException if the specified stream threw an exception during I/O
	 */
	ReadOutputStream(out mothio.OutputStream, length int32)
	//@Override
	Skip(length int64) int64
	//@Override
	SkipBytes(length int32) int32
	//@Override
	Close()
	//  {
	//  }

	/**
	 * Approximate number of bytes retained by this instance.
	 */
	GetRetainedSize() int64
	//
	// Unsupported operations
	//

	//@Override
	Mark(readLimit int32)
	//  {
	// 	 throw new UnsupportedOperationException();
	//  }

	//@Override
	Reset()
	//  {
	// 	 throw new UnsupportedOperationException();
	//  }

	//@Override
	MarkSupported() bool
	//  {
	// 	 throw new UnsupportedOperationException();
	//  }

	//@Override
	ReadChar() int8
	//  {
	// 	 throw new UnsupportedOperationException();
	//  }

	//@Override
	ReadLine() string
	//  {
	// 	 throw new UnsupportedOperationException();
	//  }

}
