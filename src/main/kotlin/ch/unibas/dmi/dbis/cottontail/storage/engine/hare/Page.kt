package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page.Constants.MASK_DIRTY
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page.Constants.MASK_PRIORITY_HIGH
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page.Constants.MASK_PRIORITY_LOW
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page.Constants.PAGE_DATA_SIZE_BYTES
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page.Constants.PAGE_HEADER_SIZE_BYTES

import ch.unibas.dmi.dbis.cottontail.utilities.extensions.optimisticRead
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write

import java.nio.ByteBuffer
import java.util.concurrent.locks.StampedLock

import kotlin.math.max

/**
 * This is a wrapper for an individual data [Page] managed by the HARE storage engine. At their core,
 * [Page]s are mere chunks of data with a fixed size of 4096 bytes each. However, each [Page] has a
 * 16 byte header used for flags and properties set by the storage engine and used internally.
 *
 * [Page]s are backed by a [ByteBuffer] object. That [ByteBuffer] must have a capacity of exactly
 * 4096 + 16 bytes, otherwise an exception will be thrown. The [ByteBuffer] backing a page is rewinded
 * when handed to the [Page]'s constructor. It is not recommended to use that [ByteBuffer] outside
 * of the [Page]s context.
 *
 * @see DiskManager
 * @see BufferPool
 *
 * @version 1.0
 * @author Ralph Gasser
 */
class Page(private val bytes: ByteBuffer) {

    /**
     * [Priority] of a [Page]. The higher, the more important it is
     */
    enum class Priority {
        HIGH, DEFAULT, LOW;
    }

    /** Some constants related to [Page]s. */
    object Constants {
        /** The size of a [Page]'s header. This value is constant.*/
        const val PAGE_HEADER_SIZE_BYTES = 16

        /** The size of a [Page]. This value is constant.*/
        const val PAGE_DATA_SIZE_BYTES = 4096

        /** Mask for the dirty flag used by [Page] objects. */
        const val MASK_DIRTY = 1 shl 0

        /** Mask for the dirty flag used by [Page] objects. */
        const val MASK_PRIORITY_HIGH = 1 shl 1

        /** Mask for the dirty flag used by [Page] objects. */
        const val MASK_PRIORITY_LOW = 1 shl 2
    }

    init {
        /** Rewind ByteBuffer. */
        this.bytes.rewind()

        /** Check capacity of ByteBuffer. */
        require(this.bytes.capacity() == PAGE_DATA_SIZE_BYTES + PAGE_HEADER_SIZE_BYTES) { throw IllegalArgumentException("A Page object must be backed by a ByteBuffer of exactly 4096 + 16 bytes.")}
    }

    /** [StampedLock] that acts as latch for concurrent read/write access to a [Page]. */
    val latch = StampedLock()

    /** Long reserved for [Page] flags. */
    var flags: Int
        get() = this.bytes.getInt(0)
        internal set(v) {
            this.bytes.putInt(0, v)
        }

    /** Internal flag: Indicating whether this [Page] has been edited and thus requires writing to disk. */
    val dirty: Boolean
        get() = (this.flags and MASK_DIRTY) == 1

    /** Internal flag: Indicates priority of this [Page] when it comes to garbage collection. */
    val priority: Priority
        get() = when {
            (this.flags and MASK_PRIORITY_HIGH == 1) -> Priority.HIGH
            (this.flags and MASK_PRIORITY_LOW == 1) -> Priority.LOW
            else -> Priority.DEFAULT
        }

    /** Internal flag: Retention counter of this page. */
    var retention: Int
        get() = this.bytes.getInt(4)
        internal set(v) {
            this.bytes.putInt(4, v)
        }

    /** Internal flag: Indicating if the retention counter of this [Page] is currently zero and it is thus elligible for garbage collection. */
    val elligibleForGc
        get() = this.retention == 0

    /** The internal identifier for the [Page]. */
    var id: PageId
        get() = this.bytes.getLong(8)
        internal set(v) {
            this.bytes.putLong(8, v)
        }

    /** The actual data (payload) held by this [Page]. */
    internal val data: ByteBuffer
        get() {
            val ret = this.bytes.position(PAGE_HEADER_SIZE_BYTES).slice()
            this.bytes.position(0)
            return ret
        }


    fun getBytes(index: Int, bytes: ByteArray) : ByteArray = this.latch.optimisticRead {
        this.bytes.position(index + PAGE_HEADER_SIZE_BYTES).get(bytes).rewind()
        return bytes
    }
    fun getBytes(index: Int, limit: Int) : ByteArray = getBytes(index, ByteArray(max(PAGE_DATA_SIZE_BYTES, limit-index)))
    fun getBytes(index: Int) : ByteArray = getBytes(index, PAGE_DATA_SIZE_BYTES)
    fun getByte(index: Int): Byte = this.latch.optimisticRead { this.bytes.get(index + PAGE_HEADER_SIZE_BYTES) }
    fun getShort(index: Int): Short = this.latch.optimisticRead {  this.bytes.getShort(index + PAGE_HEADER_SIZE_BYTES) }
    fun getChar(index: Int): Char = this.latch.optimisticRead {  this.bytes.getChar(index + PAGE_HEADER_SIZE_BYTES) }
    fun getInt(index: Int): Int = this.latch.optimisticRead {  this.bytes.getInt(index + PAGE_HEADER_SIZE_BYTES) }
    fun getLong(index: Int): Long = this.latch.optimisticRead {  this.bytes.getLong(index + PAGE_HEADER_SIZE_BYTES) }
    fun getFloat(index: Int): Float = this.latch.optimisticRead {  this.bytes.getFloat(index + PAGE_HEADER_SIZE_BYTES) }
    fun getDouble(index: Int): Double = this.latch.optimisticRead {  this.bytes.getDouble(index + PAGE_HEADER_SIZE_BYTES) }

    /**
     * Writes a [Byte] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Byte] value to write.
     * @return This [Page]
     */
    fun putByte(index: Int, value: Byte): Page = this.latch.write {
        this.bytes.put(PAGE_HEADER_SIZE_BYTES  + index, value)
        this.flags = (this.flags or MASK_DIRTY) /* Set dirty flag. */
        return this
    }

    /**
     * Writes a [ByteArray] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [ByteArray] value to write.
     * @return This [Page]
     */
    fun putBytes(index: Int, value: ByteArray): Page = this.latch.write {
        this.bytes.mark().position(PAGE_HEADER_SIZE_BYTES + index).put(value).rewind()
        this.flags = (this.flags or MASK_DIRTY)
        return this
    }

    /**
     * Writes a [Short] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Short] value to write.
     * @return This [Page]
     */
    fun putShort(index: Int, value: Short): Page = this.latch.write {
        this.bytes.putShort(PAGE_HEADER_SIZE_BYTES + index, value)
        this.flags = (this.flags or MASK_DIRTY) /* Set dirty flag. */
        return this
    }

    /**
     * Writes a [Char] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Char] value to write.
     * @return This [Page]
     */
    fun putChar(index: Int, value: Char): Page = this.latch.write {
        this.bytes.putChar(PAGE_HEADER_SIZE_BYTES + index, value)
        this.flags = (this.flags or MASK_DIRTY) /* Set dirty flag. */
        return this
    }

    /**
     * Writes a [Int] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Int] value to write.
     * @return This [Page]
     */
    fun putInt(index: Int, value: Int): Page = this.latch.write {
        this.bytes.putInt(PAGE_HEADER_SIZE_BYTES  + index, value)
        this.flags = (this.flags or MASK_DIRTY) /* Set dirty flag. */
        return this
    }

    /**
     * Writes a [Long] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Long] value to write.
     * @return This [Page]
     */
    fun putLong(index: Int, value: Long): Page = this.latch.write {
        this.bytes.putLong(PAGE_HEADER_SIZE_BYTES + index, value)
        this.flags = (this.flags or MASK_DIRTY) /* Set dirty flag. */
        return this
    }

    /**
     * Writes a [Float] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Float] value to write.
     * @return This [Page]
     */
    fun putFloat(index: Int, value: Float): Page = this.latch.write {
        this.bytes.putFloat(PAGE_HEADER_SIZE_BYTES + index, value)
        this.flags = (this.flags or MASK_DIRTY) /* Set dirty flag. */
        return this
    }

    /**
     * Writes a [Double] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Double] value to write.
     * @return This [Page]
     */
    fun putDouble(index: Int, value: Double): Page = this.latch.write {
        this.bytes.putDouble(PAGE_HEADER_SIZE_BYTES + index, value)
        this.flags = (this.flags or MASK_DIRTY) /* Set dirty flag. */
        return this
    }

    /**
     * Retains this [Page], incrementing its retention counter by one (to a minimum of zero).
     */
    internal fun retain(): Page {
        this.retention += 1
        return this
    }

    /**
     * Releases this [Page], decrementing its retention counter by one (to a minimum of zero).
     */
    internal fun release(): Page {
        this.retention = max(0, this.retention - 1)
        return this
    }
}


