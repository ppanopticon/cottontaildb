package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import java.nio.ByteBuffer

import kotlin.math.max

/**
 * This is a wrapper for an individual data [Page] managed by the HARE storage engine. At their core,
 * [Page]s are mere chunks of data  by a [ByteBuffer] with a fixed size= 2^n.
 *
 * @see DiskManager
 *
 * @version 1.1
 * @author Ralph Gasser
 */
inline class Page(private val _data: ByteBuffer) {
    /**
     * This is an internal accessor which creates a duplicate view of the [ByteBuffer] backing this [Page].
     * It should only be used by the HARE storage engine.
     */
    internal val data: ByteBuffer
        get() = this._data.duplicate().rewind()

    fun getBytes(index: Int, byteBuffer: ByteBuffer): ByteBuffer {
        byteBuffer.put(this.data.position(index).limit(index + byteBuffer.remaining()))
        return byteBuffer
    }
    fun getBytes(index: Int, bytes: ByteArray) : ByteArray {
        this.data.position(index).get(bytes).rewind()
        return bytes
    }
    fun getBytes(index: Int, limit: Int) : ByteArray = getBytes(index, ByteArray(max(this._data.capacity(), limit-index)))
    fun getBytes(index: Int) : ByteArray = getBytes(index, this._data.capacity())
    fun getByte(index: Int): Byte = this._data.get(index)
    fun getShort(index: Int): Short = this._data.getShort(index)
    fun getChar(index: Int): Char = this._data.getChar(index)
    fun getInt(index: Int): Int = this._data.getInt(index)
    fun getLong(index: Int): Long = this._data.getLong(index)
    fun getFloat(index: Int): Float = this._data.getFloat(index)
    fun getDouble(index: Int): Double =  this._data.getDouble(index)

    /**
     * Writes a [Byte] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Byte] value to write.
     * @return This [Page]
     */
    fun putByte(index: Int, value: Byte): Page {
        this._data.put(index, value)
        return this
    }

    /**
     * Writes a [ByteArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [ByteArray] value to write.
     * @return This [Page]
     */
    fun putBytes(index: Int, value: ByteArray): Page {
        this._data.position(index).put(value).rewind()
        return this
    }

    /**
     * Writes a [ByteBuffer] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [ByteArray] value to write.
     * @return This [Page]
     */
    fun putBytes(index: Int, value: ByteBuffer): Page {
        this._data.position(index).put(value).rewind()
        return this
    }

    /**
     * Writes a [Short] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Short] value to write.
     * @return This [Page]
     */
    fun putShort(index: Int, value: Short): Page {
        this._data.putShort(index, value)
        return this
    }

    /**
     * Writes a [Char] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Char] value to write.
     * @return This [Page]
     */
    fun putChar(index: Int, value: Char): Page {
        this._data.putChar(index, value)
        return this
    }

    /**
     * Writes a [Int] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Int] value to write.
     * @return This [Page]
     */
    fun putInt(index: Int, value: Int): Page {
        this._data.putInt(index, value)
        return this
    }

    /**
     * Writes a [Long] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Long] value to write.
     * @return This [Page]
     */
    fun putLong(index: Int, value: Long): Page {
        this._data.putLong(index, value)
        return this
    }

    /**
     * Writes a [Float] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Float] value to write.
     * @return This [Page]
     */
    fun putFloat(index: Int, value: Float): Page {
        this._data.putFloat(index, value)
        return this
    }

    /**
     * Writes a [Double] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Double] value to write.
     * @return This [Page]
     */
    fun putDouble(index: Int, value: Double): Page {
        this._data.putDouble(index, value)
        return this
    }

    /**
     * Clears the data in this [Page] effectively setting it to zero.
     */
    fun clear() {
        for (i in 0 until this._data.capacity()) {
            this._data.put(0, 0)
        }
    }
}


