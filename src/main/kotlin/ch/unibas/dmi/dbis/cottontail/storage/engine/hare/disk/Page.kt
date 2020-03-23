package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Constants.PAGE_DATA_SIZE_BYTES

import java.nio.ByteBuffer

import kotlin.math.max

/**
 * This is a wrapper for an individual data [Page] managed by the HARE storage engine. At their core,
 * [Page]s are mere chunks of data with a fixed size of 4096 bytes each. However, each [Page] has a
 * 16 byte header used for flags and properties set by the storage engine and used internally.
 *
 * [Page]s are backed by a [ByteBuffer] object. That [ByteBuffer] must have a capacity of exactly
 * 4096 bytes, otherwise an exception will be thrown. The [ByteBuffer] backing a page is rewinded
 * when handed to the [Page]'s constructor. It is not recommended to use that [ByteBuffer] outside
 * of the [Page]s context.
 *
 * @see DiskManager
 *
 * @version 1.0
 * @author Ralph Gasser
 */
inline class Page(val data: ByteBuffer) {

    /** Some constants related to [Page]s. */
    companion object {
        /** The size of a [Page]. This value is constant.*/
        val EMPTY = Page(ByteBuffer.allocateDirect(PAGE_DATA_SIZE_BYTES))
    }

    fun getBytes(index: Int, bytes: ByteArray) : ByteArray {
        this.data.position(index).get(bytes).rewind()
        return bytes
    }
    fun getBytes(index: Int, limit: Int) : ByteArray = getBytes(index, ByteArray(max(PAGE_DATA_SIZE_BYTES, limit-index)))
    fun getBytes(index: Int) : ByteArray = getBytes(index, PAGE_DATA_SIZE_BYTES)
    fun getByte(index: Int): Byte = this.data.get(index)
    fun getShort(index: Int): Short = this.data.getShort(index)
    fun getChar(index: Int): Char = this.data.getChar(index)
    fun getInt(index: Int): Int = this.data.getInt(index)
    fun getLong(index: Int): Long = this.data.getLong(index)
    fun getFloat(index: Int): Float = this.data.getFloat(index)
    fun getDouble(index: Int): Double =  this.data.getDouble(index)

    /**
     * Writes a [Byte] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Byte] value to write.
     * @return This [Page]
     */
    fun putByte(index: Int, value: Byte): Page {
        this.data.put(index, value)
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
        this.data.position(index).put(value).reset()
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
        this.data.position(index).put(value).reset()
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
        this.data.putShort(index, value)
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
        this.data.putChar(index, value)
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
        this.data.putInt(index, value)
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
        this.data.putLong(index, value)
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
        this.data.putFloat(index, value)
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
        this.data.putDouble(index, value)
        return this
    }

    /**
     * Clears the data in this [Page] effectively setting it to zero.
     */
    fun clear() {
        this.data.position(0).put(EMPTY.data.rewind())
    }
}


