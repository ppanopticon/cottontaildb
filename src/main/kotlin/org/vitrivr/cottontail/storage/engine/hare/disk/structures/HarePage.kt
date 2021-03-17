package org.vitrivr.cottontail.storage.engine.hare.disk.structures

import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.basics.View
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.locks.StampedLock

/**
 * This is a wrapper for an individual data [Page] managed by the HARE storage engine. At their
 * core, [HarePage]s are chunks of data in a [ByteBuffer] with a fixed size= 2^n.
 *
 * To protect [HarePage]'s from concurrent access, they expose a [StampedLock] that can be used
 * to acquire and release locks on the entire [Page]. This should be done before
 *
 * @version 1.3.1
 * @author Ralph Gasser
 */
inline class HarePage(override val buffer: ByteBuffer) : Page {

    /** The size of this [HarePage] in bytes. */
    override val size: Int
        get() = this.buffer.capacity()

    override fun getBytes(index: Int, byteBuffer: ByteBuffer): ByteBuffer {
        val buffer = this.buffer.duplicate().position(index).limit(index + byteBuffer.remaining())
        byteBuffer.put(buffer)
        return byteBuffer
    }

    override fun getBytes(index: Int, bytes: ByteArray): ByteArray {
        val buffer = this.buffer.duplicate().position(index)
        buffer.get(bytes)
        return bytes
    }

    override fun getBytes(index: Int, limit: Int) : ByteArray = getBytes(index, ByteArray(limit-index))
    override fun getBytes(index: Int) : ByteArray = getBytes(index, this.buffer.capacity())
    override fun getShorts(index: Int, array: ShortArray): ShortArray {
        val buffer = this.buffer.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.short
        }
        return array
    }

    override fun getChars(index: Int, array: CharArray): CharArray {
        val buffer = this.buffer.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.char
        }
        return array
    }

    override fun getInts(index: Int, array: IntArray): IntArray {
        val buffer = this.buffer.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.int
        }
        return array
    }

    override fun getLongs(index: Int, array: LongArray): LongArray {
        val buffer = this.buffer.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.long
        }
        return array
    }

    override fun getDoubles(index: Int, array: DoubleArray): DoubleArray {
        for (i in array.indices) {
            array[i] = this.buffer.getDouble(index + (i shl 3))
        }
        return array
    }

    override fun getFloats(index: Int, array: FloatArray): FloatArray {
        for (i in array.indices) {
            array[i] = this.buffer.getFloat(index + (i shl 2))
        }
        return array
    }

    override fun getByte(index: Int): Byte = this.buffer.get(index)
    override fun getShort(index: Int): Short = this.buffer.getShort(index)
    override fun getChar(index: Int): Char = this.buffer.getChar(index)
    override fun getInt(index: Int): Int = this.buffer.getInt(index)
    override fun getLong(index: Int): Long = this.buffer.getLong(index)
    override fun getFloat(index: Int): Float = this.buffer.getFloat(index)
    override fun getDouble(index: Int): Double = this.buffer.getDouble(index)

    /**
     * Writes a [ByteBuffer] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [ByteArray] value to write.
     * @return This [HarePage]
     */
    override fun putBytes(index: Int, value: ByteBuffer): HarePage {
        this.buffer.position(index).put(value).rewind()
        return this
    }

    /**
     * Writes a [ByteArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [ByteArray] value to write.
     * @return This [HarePage]
     */
    override fun putBytes(index: Int, value: ByteArray): HarePage {
        this.buffer.position(index).put(value).rewind()
        return this
    }

    /**
     * Writes a [ShortArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [ShortArray] value to write.
     * @return This [HarePage]
     */
    override fun putShorts(index: Int, value: ShortArray): Page {
        this.buffer.position(index)
        for (i in value.indices) {
            this.buffer.putShort(value[i])
        }
        this.buffer.rewind()
        return this
    }

    /**
     * Writes a [CharArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [CharArray] value to write.
     * @return This [HarePage]
     */
    override fun putChars(index: Int, value: CharArray): Page {
        this.buffer.position(index)
        for (i in value.indices) {
            this.buffer.putChar(value[i])
        }
        this.buffer.rewind()
        return this
    }

    /**
     * Writes an [IntArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [IntArray] value to write.
     * @return This [HarePage]
     */
    override fun putInts(index: Int, value: IntArray): Page {
        this.buffer.position(index)
        for (i in value.indices) {
            this.buffer.putInt(value[i])
        }
        this.buffer.rewind()
        return this
    }

    /**
     * Writes an [LongArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [LongArray] value to write.
     * @return This [HarePage]
     */
    override fun putLongs(index: Int, value: LongArray): Page {
        this.buffer.position(index)
        for (i in value.indices) {
            this.buffer.putLong(value[i])
        }
        this.buffer.rewind()
        return this
    }

    /**
     * Writes an [FloatArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [FloatArray] value to write.
     * @return This [HarePage]
     */
    override fun putFloats(index: Int, value: FloatArray): Page {
        for (i in value.indices) {
            this.buffer.putFloat(index + (i shl 2), value[i])
        }
        return this
    }

    /**
     * Writes an [DoubleArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [DoubleArray] value to write.
     * @return This [HarePage]
     */
    override fun putDoubles(index: Int, value: DoubleArray): Page {
        for (i in value.indices) {
            this.buffer.putDouble(index + (i shl 3), value[i])
        }
        return this
    }

    /**
     * Writes a [Byte] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Byte] value to write.
     * @return This [HarePage]
     */
    override fun putByte(index: Int, value: Byte): HarePage {
        this.buffer.put(index, value)
        return this
    }

    /**
     * Writes a [Short] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Short] value to write.
     * @return This [HarePage]
     */
    override fun putShort(index: Int, value: Short): HarePage {
        this.buffer.putShort(index, value)
        return this
    }

    /**
     * Writes a [Char] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Char] value to write.
     * @return This [HarePage]
     */
    override fun putChar(index: Int, value: Char): HarePage {
        this.buffer.putChar(index, value)
        return this
    }

    /**
     * Writes a [Int] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Int] value to write.
     * @return This [HarePage]
     */
    override fun putInt(index: Int, value: Int): HarePage {
        this.buffer.putInt(index, value)
        return this
    }

    /**
     * Writes a [Long] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Long] value to write.
     * @return This [HarePage]
     */
    override fun putLong(index: Int, value: Long): HarePage {
        this.buffer.putLong(index, value)
        return this
    }

    /**
     * Writes a [Float] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Float] value to write.
     * @return This [HarePage]
     */
    override fun putFloat(index: Int, value: Float): HarePage {
        this.buffer.putFloat(index, value)
        return this
    }

    /**
     * Writes a [Double] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Double] value to write.
     * @return This [HarePage]
     */
    override fun putDouble(index: Int, value: Double): HarePage {
        this.buffer.putDouble(index, value)
        return this
    }

    /**
     * Clears the data in this [HarePage] effectively setting it to zero.
     */
    override fun clear(): HarePage {
        for (i in 0 until this.buffer.capacity()) {
            this.buffer.put(i, 0)
        }
        return this
    }

    /**
     * Reads the content of this [HarePage] from the given [FileChannel].
     *
     * @param channel The [FileChannel] to read from.
     * @param position The position in the [FileChannel] to write to.
     */
    override fun read(channel: FileChannel, position: Long): HarePage {
        channel.read(this.buffer.clear(), position)
        return this
    }

    /**
     * Reads the content of this [HarePage] from the given [FileChannel].
     *
     * @param channel The [FileChannel] to read from.
     * @param position The position in the [FileChannel] to write to.
     */
    override fun read(channel: FileChannel): HarePage {
        channel.read(this.buffer.clear())
        return this
    }

    /**
     * Writes the content of this [HarePage] to the given [FileChannel].
     *
     * @param channel The [FileChannel] to write to.
     * @param position The position in the [FileChannel] to write to.
     */
    override fun write(channel: FileChannel, position: Long): HarePage {
        channel.write(this.buffer.clear(), position)
        return this
    }

    /**
     * Writes the content of this [HarePage] to the given [FileChannel].
     *
     * @param channel The [FileChannel] to write to.
     */
    override fun write(channel: FileChannel): View {
        channel.write(this.buffer.clear())
        return this
    }
}


