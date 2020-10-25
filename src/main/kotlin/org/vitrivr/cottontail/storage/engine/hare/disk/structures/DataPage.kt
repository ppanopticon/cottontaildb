package org.vitrivr.cottontail.storage.engine.hare.disk.structures

import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.disk.wal.WALEntry
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.shared
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.locks.StampedLock

/**
 * This is a wrapper for an individual data [DataPage] managed by the HARE storage engine. At their
 * core, [DataPage]s are chunks of data in a [ByteBuffer] with a fixed size= 2^n.
 *
 * @see org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager
 *
 * @version 1.3.0
 * @author Ralph Gasser
 */
open class DataPage(override val buffer: ByteBuffer) : Page {
    /** A [StampedLock] that mediates access to this [DataPage]'s [ByteBuffer].  */
    val lock: StampedLock = StampedLock()

    /** The size of this [DataPage] in bytes. */
    override val size: Int
        get() = this.buffer.capacity()

    override fun getBytes(index: Int, byteBuffer: ByteBuffer): ByteBuffer = this.lock.shared {
        val buffer = this.buffer.duplicate().position(index).limit(index + byteBuffer.remaining())
        byteBuffer.put(buffer)
        return byteBuffer
    }

    override fun getBytes(index: Int, bytes: ByteArray) : ByteArray = this.lock.shared {
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

    override fun getChars(index: Int, array: CharArray): CharArray = this.lock.shared {
        val buffer = this.buffer.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.char
        }
        return array
    }

    override fun getInts(index: Int, array: IntArray): IntArray = this.lock.shared {
        val buffer = this.buffer.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.int
        }
        return array
    }

    override fun getLongs(index: Int, array: LongArray): LongArray = this.lock.shared {
        val buffer = this.buffer.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.long
        }
        return array
    }

    override fun getDoubles(index: Int, array: DoubleArray): DoubleArray = this.lock.shared {
        for (i in array.indices) {
            array[i] = this.buffer.getDouble(index + (i shl 3))
        }
        return array
    }

    override fun getFloats(index: Int, array: FloatArray): FloatArray = this.lock.shared {
        for (i in array.indices) {
            array[i] = this.buffer.getFloat(index + (i shl 2))
        }
        return array
    }

    override fun getByte(index: Int): Byte = this.lock.shared { this.buffer.get(index) }
    override fun getShort(index: Int): Short = this.lock.shared { this.buffer.getShort(index) }
    override fun getChar(index: Int): Char = this.lock.shared { this.buffer.getChar(index) }
    override fun getInt(index: Int): Int = this.lock.shared { this.buffer.getInt(index) }
    override fun getLong(index: Int): Long = this.lock.shared { this.buffer.getLong(index) }
    override fun getFloat(index: Int): Float = this.lock.shared { this.buffer.getFloat(index) }
    override fun getDouble(index: Int): Double = this.lock.shared { this.buffer.getDouble(index) }

    /**
     * Writes a [ByteBuffer] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [ByteArray] value to write.
     * @return This [DataPage]
     */
    override fun putBytes(index: Int, value: ByteBuffer): DataPage = this.lock.exclusive {
        this.buffer.position(index).put(value).rewind()
        return this
    }

    /**
     * Writes a [ByteArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [ByteArray] value to write.
     * @return This [DataPage]
     */
    override fun putBytes(index: Int, value: ByteArray): DataPage = this.lock.exclusive {
        this.buffer.position(index).put(value).rewind()
        return this
    }

    /**
     * Writes a [ShortArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [ShortArray] value to write.
     * @return This [DataPage]
     */
    override fun putShorts(index: Int, value: ShortArray): Page = this.lock.exclusive {
        this.buffer.position(index)
        for (i in value.indices) {
            this.buffer.putShort(value[i])
        }
        this.buffer.rewind()
        this
    }

    /**
     * Writes a [CharArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [CharArray] value to write.
     * @return This [DataPage]
     */
    override fun putChars(index: Int, value: CharArray): Page = this.lock.exclusive {
        this.buffer.position(index)
        for (i in value.indices) {
            this.buffer.putChar(value[i])
        }
        this.buffer.rewind()
        this
    }

    /**
     * Writes an [IntArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [IntArray] value to write.
     * @return This [DataPage]
     */
    override fun putInts(index: Int, value: IntArray): Page = this.lock.exclusive {
        this.buffer.position(index)
        for (i in value.indices) {
            this.buffer.putInt(value[i])
        }
        this.buffer.rewind()
        this
    }

    /**
     * Writes an [LongArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [LongArray] value to write.
     * @return This [DataPage]
     */
    override fun putLongs(index: Int, value: LongArray): Page = this.lock.exclusive {
        this.buffer.position(index)
        for (i in value.indices) {
            this.buffer.putLong(value[i])
        }
        this.buffer.rewind()
        this
    }

    /**
     * Writes an [FloatArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [FloatArray] value to write.
     * @return This [DataPage]
     */
    override fun putFloats(index: Int, value: FloatArray): Page = this.lock.exclusive {
        for (i in value.indices) {
            this.buffer.putFloat(index + (i shl 2), value[i])
        }
        this
    }

    /**
     * Writes an [DoubleArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value [DoubleArray] value to write.
     * @return This [DataPage]
     */
    override fun putDoubles(index: Int, value: DoubleArray): Page = this.lock.exclusive {
        for (i in value.indices) {
            this.buffer.putDouble(index + (i shl 3), value[i])
        }
        this
    }

    /**
     * Writes a [Byte] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Byte] value to write.
     * @return This [DataPage]
     */
    override fun putByte(index: Int, value: Byte): DataPage = this.lock.exclusive {
        this.buffer.put(index, value)
        return this
    }

    /**
     * Writes a [Short] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Short] value to write.
     * @return This [DataPage]
     */
    override fun putShort(index: Int, value: Short): DataPage = this.lock.exclusive {
        this.buffer.putShort(index, value)
        return this
    }

    /**
     * Writes a [Char] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Char] value to write.
     * @return This [DataPage]
     */
    override fun putChar(index: Int, value: Char): DataPage = this.lock.exclusive {
        this.buffer.putChar(index, value)
        return this
    }

    /**
     * Writes a [Int] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Int] value to write.
     * @return This [DataPage]
     */
    override fun putInt(index: Int, value: Int): DataPage = this.lock.exclusive {
        this.buffer.putInt(index, value)
        return this
    }

    /**
     * Writes a [Long] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Long] value to write.
     * @return This [DataPage]
     */
    override fun putLong(index: Int, value: Long): DataPage = this.lock.exclusive {
        this.buffer.putLong(index, value)
        return this
    }

    /**
     * Writes a [Float] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Float] value to write.
     * @return This [DataPage]
     */
    override fun putFloat(index: Int, value: Float): DataPage = this.lock.exclusive {
        this.buffer.putFloat(index, value)
        return this
    }

    /**
     * Writes a [Double] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Double] value to write.
     * @return This [DataPage]
     */
    override fun putDouble(index: Int, value: Double): DataPage = this.lock.exclusive {
        this.buffer.putDouble(index, value)
        return this
    }

    /**
     * Clears the data in this [DataPage] effectively setting it to zero.
     */
    override fun clear(): DataPage = this.lock.exclusive {
        for (i in 0 until this.buffer.capacity()) {
            this.buffer.put(i, 0)
        }
        return this
    }

    /**
     * Reads the content of this [WALEntry] from disk.
     *
     * @param channel The [FileChannel] to read from.
     * @param position The position in the [FileChannel] to write to.
     */
    override fun read(channel: FileChannel, position: Long): DataPage = this.lock.exclusive {
        channel.read(this.buffer.clear(), position)
        return this
    }

    /**
     * Writes the content of this [WALEntry] to disk.
     *
     * @param channel The [FileChannel] to write to.
     * @param position The position in the [FileChannel] to write to.
     */
    override fun write(channel: FileChannel, position: Long): DataPage = this.lock.shared {
        channel.write(this.buffer.clear(), position)
        return this
    }
}


