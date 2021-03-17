package org.vitrivr.cottontail.storage.engine.hare.basics

import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool.BufferPoolPageRef
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.StampedLock

/**
 * A [PageRef] is a wrapper for [Page] managed by the HARE storage engine.
 *
 * [PageRef]s offer a facade for a (usually) finite amount of [Page]s. Furthermore,
 * [PageRef]s offer the notion of dirtiness (i.e. a [Page] that was changed by the program) and locks.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class PageRef(val id: PageId, val page: HarePage) : Page by page {

    /** Internal [StampedLock] used as latch by this [PageRef]. */
    val latch = StampedLock()

    /** Reference to the [ByteBuffer] backing this [PageRef]. */
    override val buffer: ByteBuffer
        get() = this.page.buffer

    /** Flag indicating whether or not this [PageRef] is dirty. */
    protected val _dirty = AtomicBoolean()
    val dirty
        get() = this._dirty.get()

    override fun putBytes(index: Int, value: ByteArray): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putBytes(index, value)
        return this
    }

    override fun putShorts(index: Int, value: ShortArray): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putShorts(index, value)
        return this
    }

    override fun putChars(index: Int, value: CharArray): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putChars(index, value)
        return this
    }

    override fun putInts(index: Int, value: IntArray): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putInts(index, value)
        return this
    }

    override fun putLongs(index: Int, value: LongArray): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putLongs(index, value)
        return this
    }

    override fun putFloats(index: Int, value: FloatArray): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putFloats(index, value)
        return this
    }

    override fun putDoubles(index: Int, value: DoubleArray): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putDoubles(index, value)
        return this
    }

    override fun putBytes(index: Int, value: ByteBuffer): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putBytes(index, value)
        return this
    }

    override fun putByte(index: Int, value: Byte): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putByte(index, value)
        return this
    }

    override fun putShort(index: Int, value: Short): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putShort(index, value)
        return this
    }

    override fun putChar(index: Int, value: Char): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putChar(index, value)
        return this
    }

    override fun putInt(index: Int, value: Int): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putInt(index, value)
        return this
    }

    override fun putLong(index: Int, value: Long): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putLong(index, value)
        return this
    }

    override fun putFloat(index: Int, value: Float): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putFloat(index, value)
        return this
    }

    override fun putDouble(index: Int, value: Double): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.putDouble(index, value)
        return this
    }

    override fun clear(): PageRef {
        this._dirty.compareAndSet(false, true)
        this.page.clear()
        return this
    }

    /**
     * Executes the given [action] with a read lock on this [PageRef].
     *
     * @param action The action to executed.
     */
    inline fun <T> withReadLock(action: (PageRef) -> T): T {
        val lock = this.latch.readLock()
        try {
            return action(this)
        } finally {
            this.latch.unlockRead(lock)
        }
    }

    /**
     * Executes the given [action] with a write lock on this [PageRef].
     *
     * @param action The action to executed.
     */
    inline fun <T> withWriteLock(action: (PageRef) -> T): T {
        val lock = this.latch.writeLock()
        try {
            return action(this)
        } finally {
            this.latch.unlockWrite(lock)
        }
    }

    /**
     * Flushes this [BufferPoolPageRef] to disk if it has been changed and resets the dirty flag.
     */
    abstract fun flushIfDirty()

    /**
     * Synchronizes this [PageRef] with the disk if it has been changed and resets the dirty flag.
     */
    abstract fun synchronizeIfDirty()
}