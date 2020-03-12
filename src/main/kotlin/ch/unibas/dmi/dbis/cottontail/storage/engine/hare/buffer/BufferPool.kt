package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer

import ch.unibas.dmi.dbis.cottontail.storage.basics.MemorySize
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.convertWriteLock
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.read
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.ObjectHeapIndirectPriorityQueue

import java.nio.ByteBuffer
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
class BufferPool(val disk: DirectDiskManager, val policy: EvictionPolicy = DefaultEvictionPolicy, val size: Int = 100) {

    /** Allocates direct memory as [ByteBuffer] that is used to buffer [Page]s. This is not counted towards the heap used by the JVM! */
    private val buffer = ByteBuffer.allocateDirect(this.size * Page.Constants.PAGE_DATA_SIZE_BYTES)

    /** Array of [Page]s that are kept in memory. */
    private val pages = Array(this.size) {
        Page(this.buffer.position(it * Page.Constants.PAGE_DATA_SIZE_BYTES).limit((it+1) * Page.Constants.PAGE_DATA_SIZE_BYTES).slice())
    }

    /** Array of [PageRef]s that have been generated. [PageRef]s are de-/allocated when not used. */
    private val pageRefs = Array(this.size) { PageRef(it) }

    /** */
    private val pageRefsQueue = ObjectHeapIndirectPriorityQueue(this.pageRefs, this.policy)

    /** The internal directory that maps [PageId]s to [PageRef]s.*/
    private val pageDirectory = Long2ObjectOpenHashMap<PageRef>()

    /** An internal lock that mediates access to the page directory. */
    private val directoryLock = StampedLock()

    /** The amount of memory used by this [BufferPool]. */
    val memoryUsage
        get() = MemorySize((this.size * Page.Constants.PAGE_DATA_SIZE_BYTES).toDouble())

    init {
        this.pageRefs.forEach { pageRefsQueue.enqueue(it.pointer) }
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
            val pageRef = this.pageDirectory.getOrElse(pageId) {
                stamp = this.directoryLock.convertWriteLock(stamp)
                freePage(priority)
            }

            /* Now read page from disk. */
            this.disk.read(pageId, this.pages[pageRef.pointer])

            /* Update page directory and priority queue. */
            this.pageDirectory[pageId] = pageRef

            /* Return page. */
            return pageRef
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
            val pageRef = freePage(priority)
            this.pages[pageRef.pointer].id = -1
            this.pages[pageRef.pointer].clear()

            /* Reset flags on page and read content from disk. */
            this.disk.allocate(this.pages[pageRef.pointer])

            /* Update page directory and priority queue. */
            this.pageDirectory[this.pages[pageRef.pointer].id] = pageRef

            /* Return page. */
            return pageRef
        } finally {
            this.directoryLock.unlock(stamp)
        }
    }


    /**
     * Flushes all dirty [Page]s to disk and resets their dirty flag. This method should be used with care,
     * since it will cause all pages to be written to disk.
     */
    fun flush() = this.directoryLock.read {
        for (p in this.pages) {
            if (p.dirty) {
                this.disk.update(p)
            }
        }
    }

    /**
     * Tries to find a free [PageRef] and prepares that [PageRef] for re-use by the [BufferPool].
     * This method will block, until such a [PageRef] becomes available.
     *
     * @param priority The [Priority] the new [PageRef] should have.
     * @return Index to the [PageRef] within [BufferPool.pages]
     */
    private fun freePage(priority: Priority): PageRef {
        /* Complex case: BufferPool full, PageRef needs replacement; find candidate... */
        var pageRef: PageRef? = null
        do {
            val next = this.pageRefsQueue.dequeue()
            pageRef = this.pageRefs[next]
            this.pageRefsQueue.enqueue(next)
        } while (!pageRef!!.isEligibleForGc)

        /* ... remove reference from page directory...*/
        this.pageDirectory.remove(this.pages[pageRef.pointer].id)

        /* ... and write page to disk if it's dirty. */
        if (this.pages[pageRef.pointer].dirty) {
            this.disk.update(this.pages[pageRef.pointer])
        }

        /* Update PageRefs priority and last access. */
        pageRef.priority = priority
        return pageRef
    }
    
    /**
     * A reference to a [Page] held by this [BufferPool]. These references are exposed to the upper
     * layers of the storage engine and access to a [Page] is only possible through such a [PageRef]
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class PageRef(val pointer: Int) {
        /** [PageId] of the [Page] held by this [PageRef]. */
        val id: PageId
            get() = this@BufferPool.pages[this.pointer].id

        /** Returns true if no pin is currently registered for this [PageRef]. */
        val isEligibleForGc: Boolean
            get() = !this.pin.isReadLocked && !this.pin.isWriteLocked

        /** The [Priority] of this [PageRef]. Acts as a hint for the [BufferPool]'s [EvictionPolicy]. */
        @Volatile
        var priority: Priority = Priority.UNINIT
            set(v) {
                this@BufferPool.pageRefsQueue.changed(this.pointer)
                field = v
            }

        /** The last access to this [PageRef]. Acts as a hint for the [BufferPool]'s [EvictionPolicy]. */
        @Volatile
        var lastAccess: Long = -1L
            set(v) {
                this@BufferPool.pageRefsQueue.changed(this.pointer)
                field = v
            }

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
            this@BufferPool.pages[this.pointer].putBytes(index, value)
            this
        }

        fun putByte(stamp: Long, index: Int, value: Byte): PageRef = withValidation(stamp) {
            this@BufferPool.pages[this.pointer].putByte(index, value)
            this
        }

        fun putShort(stamp: Long, index: Int, value: Short): PageRef = withValidation(stamp) {
            this@BufferPool.pages[this.pointer].putShort(index, value)
            this
        }

        fun putChar(stamp: Long, index: Int, value: Char): PageRef = withValidation(stamp) {
            this@BufferPool.pages[this.pointer].putChar(index, value)
            this
        }

        fun putInt(stamp: Long, index: Int, value: Int): PageRef = withValidation(stamp) {
            this@BufferPool.pages[this.pointer].putInt(index, value)
            this
        }

        fun putLong(stamp: Long, index: Int, value: Long): PageRef = withValidation(stamp) {
            this@BufferPool.pages[this.pointer].putLong(index, value)
            this
        }

        fun putFloat(stamp: Long, index: Int, value: Float): PageRef = withValidation(stamp) {
            this@BufferPool.pages[this.pointer].putFloat(index, value)
            this
        }

        fun putDouble(stamp: Long, index: Int, value: Double): PageRef = withValidation(stamp) {
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
         * Tries to upgrade the read lock on this [PageRef] identified by the given stamp.
         *
         * @param stamp The stamp of the read lock that should be upgrade.
         * @return New stamp
         */
        fun upgrade(stamp: Long): Long = this.pin.convertWriteLock(stamp)

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
            this@BufferPool.pageRefsQueue.changed(this.pointer)
            action()
        } else {
            throw IllegalStateException("Stamp $stamp  could not be validated. Caller is not eligible to write PageRef.")
        }
    }
}