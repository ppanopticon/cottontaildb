package ch.unibas.dmi.dbis.cottontail.storage.store

import java.io.Closeable
import java.nio.ByteBuffer

/**
 * An abstract representation over a facility that can hold data (a data [Store]) and allows for random access to that data.
 * Data may or may not be held persistently
 *
 * @version 1.0
 * @author Ralph Gasser
 */
interface Store : Closeable {
    /**
     * Writes a [Byte] value to this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param value The value to write.
     */
    operator fun set(offset: Long, value: Byte) = putByte(offset, value)

    /**
     * Reads a [Byte] value from this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @return The read value.
     */
    operator fun get(offset: Long) = getByte(offset)

    /**
     * The size of the [Store] in bytes (i.e. the maximum offset currently possible).
     */
    val size: Long

    /**
     * Read a [Double] value from this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @return The read value.
     */
    fun getDouble(offset: Long): Double

    /**
     * Writes a [Double] value to this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param value The value to write.
     */
    fun putDouble(offset: Long, value: Double)

    /**
     * Writes a [Float] value to this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param value The value to write.
     */
    fun putFloat(offset: Long, value: Float)

    /**
     * Read a [Float] value from this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @return The read value.
     */
    fun getFloat(offset: Long): Float

    /**
     * Writes a [Long] value to this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param value The value to write.
     */
    fun putLong(offset: Long, value: Long)

    /**
     * Read a [Long] value from this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @return The read value.
     */
    fun getLong(offset: Long): Long

    /**
     * Writes a [Float] value to this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param value The value to write.
     */
    fun putInt(offset: Long, value: Int)

    /**
     * Read a [Int] value from this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @return The read value.
     */
    fun getInt(offset: Long): Int

    /**
     * Writes a [Short] value to this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param value The value to write.
     */
    fun putShort(offset: Long, value: Short)

    /**
     * Read a [Short] value from this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @return The read value.
     */
    fun getShort(offset: Long): Short

    /**
     * Writes a [Char] value to this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param value The value to write.
     */
    fun putChar(offset: Long, value: Char)

    /**
     * Read a [Char] value from this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @return The read value.
     */
    fun getChar(offset: Long): Char

    /**
     * Writes a [Byte] value to this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param value The value to write.
     */
    fun putByte(offset: Long, value: Byte)

    /**
     * Read a [Byte] value from this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     */
    fun getByte(offset: Long): Byte

    /**
     * Reads a [ByteArray] value from this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param dst The destination [ByteArray] to read into.
     */
    fun getData(offset: Long, dst: ByteArray): ByteArray = getData(offset, dst, 0, dst.size)

    /**
     * Reads a [ByteArray] value from this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param dst The destination [ByteArray] to read into.
     * @param dstOffset The offset into the destination [ByteArray] (dstOffset < dst.size)
     * @param dstLength The number of bytes to write to the destination array (dstOffset + dstLength < dst.size).
     */
    fun getData(offset: Long, dst: ByteArray, dstOffset: Int, dstLength: Int): ByteArray {
        val buffer = ByteBuffer.wrap(dst).position(dstOffset).limit(dstLength)
        this.getData(offset, buffer)
        return buffer.array()
    }

    /**
     * Reads a [ByteArray] value from this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param dst The destination [ByteArray] to read into.
     */
    fun getData(offset: Long, dst: ByteBuffer): ByteBuffer


    /**
     * Writes a [ByteArray] value to this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param src The values to write.
     */
    fun putData(offset: Long, src: ByteArray) = putData(offset, src, 0, src.size)

    /**
     * Writes a [ByteArray] value to this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param src The [ByteArray] that contains the values to write.
     * @param srcOffset The offset into the source array (inclusive, srcOffset < src.size).
     * @param srcLength The number of bytes to read from the source array (srcOffset + srcLength < src.size).
     */
    fun putData(offset: Long, src: ByteArray, srcOffset: Int, srcLength: Int) = putData(offset, ByteBuffer.wrap(src, srcOffset, srcLength))

    /**
     * Writes a [ByteBuffer] value to this [Store]
     *
     * @param offset The offset into the [Store] in bytes.
     * @param src The [ByteBuffer] that contains the values to write.
     */
    fun putData(offset: Long, src: ByteBuffer)

    /**
     * Clears the provided range of this [Store] and sets its bytes to zero.
     *
     * @param range The desired range to clear.
     */
    fun clear(range: LongRange)

    /**
     * Truncates this [Store] to the given size. The provided size must be smaller than the [Store]'s current size,
     * otherwise invocation of this method will have no effect.
     *
     * @param size The desired size of the [Store] in bytes.
     */
    fun truncate(size: Long)

    /**
     * Grows this [Store] so as to accommodate at least the given offset in bytes. Depending on the implementation, the
     * underlying [Store] may or may not grow physically, after invoking this method. However, the implementation must make
     * sure, that subsequent calls to any getXYZ yor putXYZ method is save.
     *
     * @param offset The offset to which to grow this [Store].
     */
    fun grow(offset: Long)

    /**
     * Copies a portion of the data contain in this [Store] to the provided target [Store]
     *
     * @param inputOffset The offset in bytes into the source [Store].
     * @param target The target [Store]
     * @param targetOffset The offset in bytes into the target [Store]
     * @param size The amount of data to copy.
     */
    fun copyTo(inputOffset: Long, target: Store, targetOffset: Long, size: Int) {
        val data = this.getData(inputOffset, ByteArray(size = size))
        target.putData(targetOffset, data)
    }

    /**
     * Returns true, if this [Store] is open and false otherwise.
     *
     * @return True if this [Store] is open, false otherwise.
     */
    val isOpen: Boolean

    /** True if this [Store] implementation is backed by persistent storage. */
    val isPersistent: Boolean
        get() = false
}