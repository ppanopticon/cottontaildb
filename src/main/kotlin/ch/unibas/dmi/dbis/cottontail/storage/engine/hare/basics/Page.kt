package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics
import java.nio.ByteBuffer

/**
 * A [Page] is a wrapper for an individual data [Page] managed by the HARE storage engine. At their
 * core, [Page]s are mere chunks of data usually backed by a [ByteBuffer] with a fixed size= 2^n.
 *
 * @version 1.1
 * @author Ralph Gasser
 */
interface Page {

    /** Size of this [Page] in bytes. */
    val size: Int


    /**
     *
     */
    fun <T> read(index: Int, action: (ByteBuffer) -> T): T

    fun getBytes(index: Int, byteBuffer: ByteBuffer): ByteBuffer
    fun getBytes(index: Int, bytes: ByteArray) : ByteArray
    fun getBytes(index: Int, limit: Int) : ByteArray
    fun getBytes(index: Int) : ByteArray
    fun getShorts(index: Int, array: ShortArray): ShortArray
    fun getChars(index: Int, array: CharArray): CharArray
    fun getInts(index: Int, array: IntArray): IntArray
    fun getLongs(index: Int, array: LongArray): LongArray
    fun getDoubles(index: Int, array: DoubleArray): DoubleArray
    fun getFloats(index: Int, array: FloatArray): FloatArray
    fun getByte(index: Int): Byte
    fun getShort(index: Int): Short
    fun getChar(index: Int): Char
    fun getInt(index: Int): Int
    fun getLong(index: Int): Long
    fun getFloat(index: Int): Float
    fun getDouble(index: Int): Double


    /**
     *
     */
    fun <T> write(index: Int, action: (ByteBuffer) -> T) : T

    /**
     * Writes a [ByteBuffer] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [ByteArray] value to write.
     * @return This [Page]
     */
    fun putBytes(index: Int, value: ByteBuffer): Page

    /**
     * Writes a [ByteArray] to the given position.
     *
     * @param index Position to write [ByteArray] to.
     * @param value [ByteArray] value to write.
     * @return This [Page]
     */
    fun putBytes(index: Int, value: ByteArray): Page

    /**
     * Writes a [ShortArray] to the given position.
     *
     * @param index Position to write [ShortArray] to.
     * @param value [ShortArray] value to write.
     * @return This [Page]
     */
    fun putShorts(index: Int, value: ShortArray): Page

    /**
     * Writes an [CharArray] to the given position.
     *
     * @param index Position to write [CharArray] to.
     * @param value [CharArray] value to write.
     * @return This [Page]
     */
    fun putChars(index: Int, value: CharArray): Page

    /**
     * Writes an [IntArray] to the given position.
     *
     * @param index Position to write [IntArray] to.
     * @param value [IntArray] value to write.
     * @return This [Page]
     */
    fun putInts(index: Int, value: IntArray): Page

    /**
     * Writes a [LongArray] to the given position.
     *
     * @param index Position to write [LongArray] to.
     * @param value [LongArray] value to write.
     * @return This [Page]
     */
    fun putLongs(index: Int, value: LongArray): Page

    /**
     * Writes a [FloatArray] to the given position.
     *
     * @param index Position to write [FloatArray] to.
     * @param value [FloatArray] value to write.
     * @return This [Page]
     */
    fun putFloats(index: Int, value: FloatArray): Page

    /**
     * Writes a [DoubleArray] to the given position.
     *
     * @param index Position to write [DoubleArray] to.
     * @param value [DoubleArray] value to write.
     * @return This [Page]
     */
    fun putDoubles(index: Int, value: DoubleArray): Page

    /**
     * Writes a [Byte] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Byte] value to write.
     * @return This [Page]
     */
    fun putByte(index: Int, value: Byte): Page

    /**
     * Writes a [Short] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Short] value to write.
     * @return This [Page]
     */
    fun putShort(index: Int, value: Short): Page

    /**
     * Writes a [Char] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Char] value to write.
     * @return This [Page]
     */
    fun putChar(index: Int, value: Char): Page

    /**
     * Writes a [Int] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Int] value to write.
     * @return This [Page]
     */
    fun putInt(index: Int, value: Int): Page

    /**
     * Writes a [Long] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Long] value to write.
     * @return This [Page]
     */
    fun putLong(index: Int, value: Long): Page

    /**
     * Writes a [Float] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Float] value to write.
     * @return This [Page]
     */
    fun putFloat(index: Int, value: Float): Page

    /**
     * Writes a [Double] to the given position.
     *
     * @param index Position to write byte to.
     * @param value New [Double] value to write.
     * @return This [Page]
     */
    fun putDouble(index: Int, value: Double): Page

    /**
     * Clears the data in this [Page] effectively setting it to zero.
     */
    fun clear(): Page
}