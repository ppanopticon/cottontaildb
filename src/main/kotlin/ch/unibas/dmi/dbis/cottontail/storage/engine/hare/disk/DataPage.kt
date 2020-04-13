package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.exclusive
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.shared
import java.nio.ByteBuffer
import java.util.concurrent.locks.StampedLock

/**
 * This is a wrapper for an individual data [DataPage] managed by the HARE storage engine. At their core,
 * [DataPage]s are mere chunks of data  by a [ByteBuffer] with a fixed size= 2^n.
 *
 * @see DiskManager
 *
 * @version 1.2
 * @author Ralph Gasser
 */
class DataPage(internal val _data: ByteBuffer) : Page {

    init {
        this._data.clear() /* Safety measure: ByteBuffer is reset. */
    }

    /** A [StampedLock] that mediates access to this [DataPage]'s [ByteBuffer].  */
    internal val lock: StampedLock = StampedLock()

    override fun getBytes(index: Int, byteBuffer: ByteBuffer): ByteBuffer = this.lock.shared {
        val buffer = this._data.duplicate().position(index).limit(index + byteBuffer.remaining())
        byteBuffer.put(buffer)
        return byteBuffer
    }
    override fun getBytes(index: Int, bytes: ByteArray) : ByteArray = this.lock.shared {
        val buffer = this._data.duplicate().position(index)
        buffer.get(bytes)
        return bytes
    }
    override fun getBytes(index: Int, limit: Int) : ByteArray = getBytes(index, ByteArray(limit-index))
    override fun getBytes(index: Int) : ByteArray = getBytes(index, this._data.capacity())
    override fun getShorts(index: Int, array: ShortArray): ShortArray {
        val buffer = this._data.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.short
        }
        return array
    }

    override fun getChars(index: Int, array: CharArray): CharArray {
        val buffer = this._data.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.char
        }
        return array
    }

    override fun getInts(index: Int, array: IntArray): IntArray {
        val buffer = this._data.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.int
        }
        return array
    }

    override fun getLongs(index: Int, array: LongArray): LongArray {
        val buffer = this._data.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.long
        }
        return array
    }

    override fun getDoubles(index: Int, array: DoubleArray): DoubleArray = this.lock.shared {
        val buffer = this._data.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.double
        }
        return array
    }

    override fun getFloats(index: Int, array: FloatArray): FloatArray = this.lock.shared {
        val buffer = this._data.duplicate().position(index)
        for (i in array.indices) {
            array[i] = buffer.float
        }
        return array
    }

    override fun getByte(index: Int): Byte = this.lock.shared { this._data.get(index) }
    override fun getShort(index: Int): Short = this.lock.shared { this._data.getShort(index) }
    override fun getChar(index: Int): Char = this.lock.shared { this._data.getChar(index) }
    override fun getInt(index: Int): Int = this.lock.shared { this._data.getInt(index) }
    override fun getLong(index: Int): Long = this.lock.shared { this._data.getLong(index) }
    override fun getFloat(index: Int): Float = this.lock.shared { this._data.getFloat(index) }
    override fun getDouble(index: Int): Double = this.lock.shared { this._data.getDouble(index) }

    /**
     * Writes a [ByteBuffer] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [ByteArray] value to write.
     * @return This [DataPage]
     */
    override fun putBytes(index: Int, value: ByteBuffer): DataPage = this.lock.exclusive {
        this._data.position(index).put(value).rewind()
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
        this._data.position(index).put(value).rewind()
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
        this._data.position(index)
        for (i in value.indices) {
            this._data.putShort(value[i])
        }
        this._data.rewind()
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
        this._data.position(index)
        for (i in value.indices) {
            this._data.putInt(value[i])
        }
        this._data.rewind()
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
        this._data.position(index)
        for (i in value.indices) {
            this._data.putLong(value[i])
        }
        this._data.rewind()
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
        this._data.position(index)
        for (i in value.indices) {
            this._data.putFloat(value[i])
        }
        this._data.rewind()
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
        this._data.position(index)
        for (i in value.indices) {
            this._data.putDouble(value[i])
        }
        this._data.rewind()
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
        this._data.put(index, value)
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
        this._data.putShort(index, value)
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
        this._data.putChar(index, value)
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
        this._data.putInt(index, value)
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
        this._data.putLong(index, value)
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
        this._data.putFloat(index, value)
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
        this._data.putDouble(index, value)
        return this
    }

    /**
     * Clears the data in this [DataPage] effectively setting it to zero.
     */
    override fun clear(): DataPage = this.lock.exclusive {
        for (i in 0 until this._data.capacity()) {
            this._data.put(0, 0)
        }
        return this
    }
}


