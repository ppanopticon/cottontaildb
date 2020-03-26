package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics
import java.nio.ByteBuffer

/**
 * A [Page] is a wrapper for an individual data [Page] managed by the HARE storage engine. At their
 * core, [Page]s are mere chunks of data usually backed by a [ByteBuffer] with a fixed size= 2^n.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
interface Page {
    fun getBytes(index: Int, byteBuffer: ByteBuffer): ByteBuffer
    fun getBytes(index: Int, bytes: ByteArray) : ByteArray
    fun getBytes(index: Int, limit: Int) : ByteArray
    fun getBytes(index: Int) : ByteArray
    fun getByte(index: Int): Byte
    fun getShort(index: Int): Short
    fun getChar(index: Int): Char
    fun getInt(index: Int): Int
    fun getLong(index: Int): Long
    fun getFloat(index: Int): Float
    fun getDouble(index: Int): Double

    /**
     * Writes a [Byte] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Byte] value to write.
     * @return This [DataPage]
     */
    fun putByte(index: Int, value: Byte): Page

    /**
     * Writes a [ByteArray] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [ByteArray] value to write.
     * @return This [DataPage]
     */
    fun putBytes(index: Int, value: ByteArray): Page

    /**
     * Writes a [ByteBuffer] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [ByteArray] value to write.
     * @return This [DataPage]
     */
    fun putBytes(index: Int, value: ByteBuffer): Page

    /**
     * Writes a [Short] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Short] value to write.
     * @return This [DataPage]
     */
    fun putShort(index: Int, value: Short): Page

    /**
     * Writes a [Char] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Char] value to write.
     * @return This [DataPage]
     */
    fun putChar(index: Int, value: Char): Page

    /**
     * Writes a [Int] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Int] value to write.
     * @return This [DataPage]
     */
    fun putInt(index: Int, value: Int): Page

    /**
     * Writes a [Long] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Long] value to write.
     * @return This [DataPage]
     */
    fun putLong(index: Int, value: Long): Page

    /**
     * Writes a [Float] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Float] value to write.
     * @return This [DataPage]
     */
    fun putFloat(index: Int, value: Float): Page

    /**
     * Writes a [Double] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Double] value to write.
     * @return This [DataPage]
     */
    fun putDouble(index: Int, value: Double): Page

    /**
     * Clears the data in this [DataPage] effectively setting it to zero.
     */
    fun clear(): Page
}