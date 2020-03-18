package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer

import ch.unibas.dmi.dbis.cottontail.storage.basics.MemorySize
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Constants.PAGE_DATA_SIZE_BYTES
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.convertWriteLock
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.read

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * A [BufferPool] mediates access to a HARE file through a [DirectDiskManager] and facilitates reading and
 * writing [Page]s from/to memory and swapping [Page]s into the in-memory buffer.
 *
 * @see DirectDiskManager
 *
 * @version 1.0
 * @author Ralph Gasser
 */
class BufferPool(val disk: DirectDiskManager, private val policy: EvictionPolicy = DefaultEvictionPolicy, val size: Int = 100) {

    /** Allocates direct memory as [ByteBuffer] that is used to buffer [Page]s. This is not counted towards the heap used by the JVM! */
    private val buffer = ByteBuffer.allocateDirect(this.size * PAGE_DATA_SIZE_BYTES)

    /** Array of [Page]s that are kept in memory. */
    private val pages = Array(this.size) {
        Page(this.buffer.position(it * PAGE_DATA_SIZE_BYTES).limit((it+1) * PAGE_DATA_SIZE_BYTES).slice())
    }

    /** Internal queue that is maintained to get the [PageRef] that can be evicted next! */
    private val pageRefsQueue = PriorityQueue(this.pages.size, this.policy)

    /** The internal directory that maps [PageId]s to [PageRef]s.*/
    private val pageDirectory = Long2ObjectOpenHashMap<PageRef>()

    /** An internal lock that mediates access to the page directory. */
    private val directoryLock = StampedLock()

    /** The amount of memory used by this [BufferPool]. */
    val memoryUsage
        get() = MemorySize((this.size * PAGE_DATA_SIZE_BYTES).toDouble())

    init {
        for (i in this.pages.indices) {
            this.pageRefsQueue.offer(PageRef(-1L, i, Priority.UNINIT))
        }
    }

    /**
     * Reads the [Page] identified by the given [PageId]. If a [PageRef] for the requested [Page]
     * exists in the [BufferPool], that [PageRef] is returned. Otherwise, the [Page] is read from
     * underlying storage.
     *
     * @param pageId The [PageId] of the requested [Page]
     * @param priority A [Priority] hint for the new [PageRef]. Acts as a hint to the [EvictionPolicy].
     * @return [PageRef] for the requested [Page]
     */
    fun get(pageId: PageId, priority: Priority = Priority.DEFAULT): PageRef {
        var stamp = this.directoryLock.readLock() /* Acquire non-exclusive lock. */
        try {
            val oldRef = this.pageDirectory.getOrElse(pageId) {
                stamp = this.directoryLock.convertWriteLock(stamp)
                freePage()
            }

            /* Now read page from disk. */
            this.disk.read(pageId, this.pages[oldRef.pointer])

            /* Update page directory and queue and return new PageRef. */
            val newRef = PageRef(pageId, oldRef.pointer, priority)
            this.pageDirectory[pageId] = newRef
            this.pageRefsQueue.offer(newRef)
            return newRef
        } finally {
            this.directoryLock.unlock(stamp)
        }
    }

    /**
     * Appends a new [Page] to the file managed by this [BufferPool] and returns a [PageRef] for that [Page]
     *
     * @param priority A [Priority] hint for the new [PageRef]. Acts as a hint to the [EvictionPolicy].
     * @return [PageRef] for the appended [Page]
     */
    fun append(priority: Priority = Priority.DEFAULT): PageRef {
        val stamp = this.directoryLock.writeLock() /* Acquire exclusive lock. */
        try {
            /* Get next free page object. */
            val oldRef = freePage()

            /* Clear page and allocate new page on disk. */
            val pageId = this.disk.allocate()
            this.pages[oldRef.pointer].clear()

            /* Update page directory and queue and return new PageRef. */
            val newRef = PageRef(pageId, oldRef.pointer, priority)
            this.pageDirectory[pageId] = newRef
            this.pageRefsQueue.offer(newRef)
            return newRef
        } finally {
            this.directoryLock.unlock(stamp)
        }
    }

    /**
     * Flushes all dirty [PageRef]s to disk and resets their dirty flag. This method should be used
     * with care, since it will cause all [Page]s to be written to disk.
     */
    fun flush() = this.directoryLock.read {
        for (p in this.pageDirectory.values) {
            if (p.dirty) {
                this.disk.update(p.id, this.pages[p.pointer])
            }
        }
    }

    /**
     * Resets all dirty [PageRef]s to their state as stored on disk thus resetting their dirty flag. This
     * method should be used with care, since it will cause all [Page]s to be read from disk.
     */
    fun reset() = this.directoryLock.read {
        for (p in this.pageDirectory.values) {
            if (p.dirty) {
                this.disk.read(p.id, this.pages[p.pointer])
            }
        }
    }

    /**
     * Tries to find a free [PageRef] and prepares that [PageRef] for re-use by the [BufferPool].
     * This method will block, until such a [PageRef] becomes available.
     *
     * @return Index to the [PageRef] within [BufferPool.pages]
     */
    private fun freePage(): PageRef {
        /* Complex case: BufferPool full, PageRef needs replacement; find candidate... */
        var pageRef: PageRef?
        do {
            pageRef = this.pageRefsQueue.poll()
            if (!pageRef.isEligibleForGc) {
                this.pageRefsQueue.offer(pageRef)
            }
        } while (!pageRef!!.isEligibleForGc)

        /* ... remove reference from page directory...*/
        this.pageDirectory.remove(pageRef.id)

        /* ... and write page to disk if it's dirty. */
        if (pageRef.dirty) {
            this.disk.update(pageRef.id, this.pages[pageRef.pointer])
        }

        /* Update PageRefs priority and last access. */
        return pageRef
    }
    
    /**
     * A reference to a [Page] held by this [BufferPool]. These references are exposed to the upper
     * layers of the storage engine and access to a [Page] is only possible through such a [PageRef]
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class PageRef(val id: PageId, val pointer: Int, val priority: Priority) {

        /** Returns true if no pin is currently registered for this [PageRef]. */
        val isEligibleForGc: Boolean
            get() = !this.pin.isReadLocked && !this.pin.isWriteLocked

        /** Flag indicating whether or not this [PageRef] is dirty. */
        @Volatile
        var dirty = false

        /** Counter that counts how often this [PageRef] was accessed. */
        @Volatile
        var accessed: Long = 0L
            private set

        /** The last access to this [PageRef]. Acts as a hint for the [BufferPool]'s [EvictionPolicy]. */
        @Volatile
        var lastAccess: Long = System.currentTimeMillis()
            private set

        /** [StampedLock] that acts as latch for concurrent read/write access to a [Page]. */
        private val pin = StampedLock()

        fun getBytes(stamp: Long, index: Int, bytes: ByteArray) : ByteArray = withValidation(stamp) {
            this@BufferPool.pages[this.pointer].getBytes(index, bytes)
        }
        fun getBytes(stamp: Long, index: Int, limit: Int) : ByteArray = withValidation(stamp) {
            this@BufferPool.pages[this.pointer].getBytes(index, limit)
        }
        fun getBytes(stamp: Long, index: Int) : ByteArray = withValidation(stamp) {
            this@BufferPool.pages[this.pointer].getBytes(index)
        }
        fun getByte(stamp: Long, index: Int): Byte = withValidation(stamp) {
            this@BufferPool.pages[this.pointer].getByte(index)
        }
        fun getShort(stamp: Long, index: Int): Short = withValidation(stamp) {  this@BufferPool.pages[this.pointer].getShort(index) }
        fun getChar(stamp: Long, index: Int): Char = withValidation(stamp) { this@BufferPool.pages[this.pointer].getChar(index) }
        fun getInt(stamp: Long, index: Int): Int = withValidation(stamp) {  this@BufferPool.pages[this.pointer].getInt(index) }
        fun getLong(stamp: Long, index: Int): Long = withValidation(stamp) {  this@BufferPool.pages[this.pointer].getLong(index) }
        fun getFloat(stamp: Long, index: Int): Float = withValidation(stamp) {  this@BufferPool.pages[this.pointer].getFloat(index) }
        fun getDouble(stamp: Long, index: Int): Double = withValidation(stamp) { this@BufferPool.pages[this.pointer].getDouble(index) }

        fun putBytes(stamp: Long, index: Int, value: ByteArray): PageRef = withValidation(stamp) {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putBytes(index, value)
            this
        }

        fun putByte(stamp: Long, index: Int, value: Byte): PageRef = withValidation(stamp) {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putByte(index, value)
            this
        }

        fun putShort(stamp: Long, index: Int, value: Short): PageRef = withValidation(stamp) {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putShort(index, value)
            this
        }

        fun putChar(stamp: Long, index: Int, value: Char): PageRef = withValidation(stamp) {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putChar(index, value)
            this
        }

        fun putInt(stamp: Long, index: Int, value: Int): PageRef = withValidation(stamp) {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putInt(index, value)
            this
        }

        fun putLong(stamp: Long, index: Int, value: Long): PageRef = withValidation(stamp) {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putLong(index, value)
            this
        }

        fun putFloat(stamp: Long, index: Int, value: Float): PageRef = withValidation(stamp) {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putFloat(index, value)
            this
        }

        fun putDouble(stamp: Long, index: Int, value: Double): PageRef = withValidation(stamp) {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putDouble(index, value)
            this
        }

        /**
         * Retains the [Page] referenced by this [PageRef] by obtaining a lock.Retaining a [Page]
         * transfers ownership of the [PageRef] to the caller. Owner must release the [PageRef],
         * once they're done working with it.
         *
         * @param write True, if a write lock is requested, false otherwise.
         * @return Stamp for the lock. Must be provided in order to release the [PageRef].
         */
        fun retain(write: Boolean): Long = if (write) {
            this.pin.writeLock()
        } else {
            this.pin.readLock()
        }

        /**
         * Releases the lock on this [PageRef] identified by the given stamp.
         *
         * @param stamp The stamp that identifies the lock that should be released.
         */
        fun release(stamp: Long) = this.pin.unlock(stamp)

        /**
         * Helper function that validates the given stamp and registers access to this [PageRef].
         * This is usually executed before read/write access to the underlying page.
         *
         * @param stamp The stamp that should be checked.
         */
        private inline fun <R> withValidation(stamp: Long, action: () -> R) = if (this.pin.validate(stamp)) {
            this.lastAccess = System.currentTimeMillis()
            this.accessed += 1
            action()
        } else {
            throw IllegalStateException("Stamp $stamp  could not be validated. Caller is not eligible to write PageRef.")
        }
    }
}