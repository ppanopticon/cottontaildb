package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer

import ch.unibas.dmi.dbis.cottontail.storage.basics.MemorySize
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Resource
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Constants.PAGE_DATA_SIZE_BYTES
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.read
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock

/**
 * A [BufferPool] mediates access to a HARE file through a [DirectDiskManager] and facilitates reading and
 * writing [Page]s from/to memory and swapping [Page]s into the in-memory buffer.
 *
 * @see DirectDiskManager
 *
 * @version 1.1
 * @author Ralph Gasser
 */
class BufferPool(private val disk: DiskManager, private val policy: EvictionPolicy = DefaultEvictionPolicy, private val size: Int = 100): Resource {

    /** Allocates direct memory as [ByteBuffer] that is used to buffer [Page]s. This is not counted towards the heap used by the JVM! */
    private val buffer = ByteBuffer.allocateDirect(this.size * PAGE_DATA_SIZE_BYTES)

    /** Array of [Page]s that are kept in memory. */
    private val pages = Array(this.size) {
        Page(this.buffer.position(it * PAGE_DATA_SIZE_BYTES).limit((it+1) * PAGE_DATA_SIZE_BYTES).slice())
    }

    /** Array of [StampedLock]s that are being held. */
    private val pins = IntArray(this.size)

    /** The internal directory that maps [PageId]s to [PageRef]s.*/
    private val pageDirectory = Long2ObjectOpenHashMap<PageRef>()

    /** Internal set that holds all [PageRef]s that are ready to be collected! */
    private val referenceQueue = PriorityQueue(this.size, this.policy)

    /** An internal lock that mediates access to the [BufferPool.pageDirectory]. */
    private val directoryLock = StampedLock()

    /** A [ReentrantReadWriteLock] that mediates access to the closed state of this [BufferPool]. */
    private val closeLock = StampedLock()

    /** Return true if this [DiskManager] and thus this [BufferPool] is still open. */
    override val isOpen: Boolean
        get() = this.disk.isOpen

    /** Physical size of the HARE page file underpinning this [BufferPool]. */
    val diskSize = this.disk.size

    /** The amount of memory used by this [BufferPool] to buffer [PageRef]s. */
    val memorySize
        get() = MemorySize((this.size * PAGE_DATA_SIZE_BYTES).toDouble())

    /** Returns the total number of buffered [Page]s. */
    val bufferedPages
        get() = this.pageDirectory.size

    /** Returns the total number of [Page]s stored in the HARE Page file underpinning this [BufferPool]. */
    val totalPages
        get() = this.disk.pages

    init {
        for (i in 0 until this.size) {
            PageRef(-1L, i, Priority.UNINIT)
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
    fun get(pageId: PageId, priority: Priority = Priority.DEFAULT): PageRef = this.closeLock.read {
        check(this.disk.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        var directoryStamp = this.directoryLock.readLock()  /* Acquire non-exclusive lock to close lock.  */
        try {
            val ref = this.pageDirectory.getOrElse(pageId) {
                directoryStamp = this.directoryLock.tryConvertToWriteLock(directoryStamp) /* Upgrade to exclusive lock */
                val oldRef = freePage()

                /* Now read page from disk. */
                this.disk.read(pageId, this.pages[oldRef])

                /* Update page directory and queue and return new PageRef. */
                val newRef = PageRef(pageId, oldRef, priority)
                this.pageDirectory[pageId] = newRef
                newRef
            }

            /* Increment retention counter for new PageRef. */
            synchronized(this.pins[ref.pointer]) {
                this.pins[ref.pointer] += 1
                ref.touch()
            }
            return ref
        } finally {
            this.directoryLock.unlock(directoryStamp)
        }
    }

    /**
     * Appends a new [Page] to the file managed by this [BufferPool] and returns a [PageRef] for that [Page]
     *
     * @param priority A [Priority] hint for the new [PageRef]. Acts as a hint to the [EvictionPolicy].
     * @return [PageRef] for the appended [Page]
     */
    fun append(priority: Priority = Priority.DEFAULT): PageRef = this.closeLock.read {
        check(this.disk.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.write {
            /* Get next free page object. */
            val oldRef = freePage()

            /* Clear page and allocate new page on disk. */
            val pageId = this.disk.allocate()
            this.pages[oldRef].clear()

            /* Update page directory and queue and return new PageRef. */
            val ref = PageRef(pageId, oldRef, priority)
            this.pageDirectory[pageId] = ref

            /* Increment retention counter for new PageRef. */
            synchronized(this.pins[ref.pointer]) {
                this.pins[ref.pointer] += 1
                ref.touch()
            }
            return ref
        }
    }

    /**
     * Flushes all dirty [PageRef]s to disk and resets their dirty flag. This method should be used
     * with care, since it will cause all [Page]s to be written to disk.
     */
    fun flush() = this.closeLock.read {
        check(this.disk.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.read {
            for (p in this.pageDirectory.values) {
                if (p.dirty) {
                    this.disk.update(p.id, this.pages[p.pointer])
                }
            }
        }
    }

    /**
     * Synchronizes all dirty [PageRef]s with the version on disk thus resetting their dirty flag.
     * This method should be used with care, since it will cause all [Page]s to be read from disk.
     */
    fun synchronize() = this.closeLock.read {
        check(this.disk.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.read {
            for (p in this.pageDirectory.values) {
                if (p.dirty) {
                    this.disk.read(p.id, this.pages[p.pointer])
                }
            }
        }
    }

    /** Closes this [BufferPool] and the underlying [DiskManager]. */
    override fun close() = this.closeLock.write {
        if (this.isOpen) {
            this.disk.close()
        }
    }

    /**
     * Tries to find a free [PageRef] and prepares that [PageRef] for re-use by the [BufferPool].
     * This method will block, until such a [PageRef] becomes available.
     *
     * @return Index to the [PageRef] within [BufferPool.pages]
     */
    private fun freePage(): Int {
        do {
            val pageRef = this.referenceQueue.poll()
            if (pageRef != null) {
                synchronized(this.pins[pageRef.pointer]) {
                    if (this.pins[pageRef.pointer] > 0) {
                        pageRef.touch()
                        this.referenceQueue.offer(pageRef)
                    } else {
                        /* Remove PageRef from directory... */
                        this.pageDirectory.remove(pageRef.id)
                    }
                }

                /* ... and write page to disk if it's dirty. */
                if (this.pins[pageRef.pointer] == 0) {
                    if (pageRef.dirty) {
                        this.disk.update(pageRef.id, this.pages[pageRef.pointer])
                    }
                    return pageRef.pointer
                }
            }
            Thread.onSpinWait()
        } while (true)
    }

    /**
     * A reference to a [Page] held by this [BufferPool]. These references are exposed to the upper
     * layers of the storage engine and access to a [Page] is only possible through such a [PageRef]
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class PageRef(val id: PageId, val pointer: Int, val priority: Priority) {

        init {
            this@BufferPool.referenceQueue.offer(this)
        }

        /** Flag indicating whether or not this [PageRef] is dirty. */
        @Volatile
        var dirty = false
            private set

        /** Counter that counts how often this [PageRef] was accessed. */
        @Volatile
        var accessed: Long = 0L
            private set

        /** The last access to this [PageRef]. Acts as a hint for the [BufferPool]'s [EvictionPolicy]. */
        @Volatile
        var lastAccess: Long = System.currentTimeMillis()
            private set

        fun getBytes(index: Int, bytes: ByteBuffer): ByteBuffer =  this@BufferPool.pages[this.pointer].getBytes(index, bytes)
        fun getBytes(index: Int, bytes: ByteArray): ByteArray = this@BufferPool.pages[this.pointer].getBytes(index, bytes)
        fun getBytes(index: Int, limit: Int): ByteArray = this@BufferPool.pages[this.pointer].getBytes(index, limit)
        fun getBytes(index: Int): ByteArray = this@BufferPool.pages[this.pointer].getBytes(index)
        fun getByte(index: Int): Byte = this@BufferPool.pages[this.pointer].getByte(index)
        fun getShort(index: Int): Short = this@BufferPool.pages[this.pointer].getShort(index)
        fun getChar(index: Int): Char = this@BufferPool.pages[this.pointer].getChar(index)
        fun getInt(index: Int): Int = this@BufferPool.pages[this.pointer].getInt(index)
        fun getLong(index: Int): Long = this@BufferPool.pages[this.pointer].getLong(index)
        fun getFloat(index: Int): Float =  this@BufferPool.pages[this.pointer].getFloat(index)
        fun getDouble(index: Int): Double = this@BufferPool.pages[this.pointer].getDouble(index)

        fun putBytes(index: Int, value: ByteArray): PageRef {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putBytes(index, value)
            return this
        }

        fun putBytes(index: Int, value: ByteBuffer): PageRef {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putBytes(index, value)
            return this
        }

        fun putByte(index: Int, value: Byte): PageRef {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putByte(index, value)
            return this

        }

        fun putShort(index: Int, value: Short): PageRef {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putShort(index, value)
            return this
        }

        fun putChar(index: Int, value: Char): PageRef {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putChar(index, value)
            return this
        }

        fun putInt(index: Int, value: Int): PageRef {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putInt(index, value)
            return this
        }

        fun putLong(index: Int, value: Long): PageRef {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putLong(index, value)
            return this
        }

        fun putFloat(index: Int, value: Float): PageRef {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putFloat(index, value)
            return this

        }

        fun putDouble(index: Int, value: Double): PageRef {
            this.dirty = true
            this@BufferPool.pages[this.pointer].putDouble(index, value)
            return this
        }

        /**
         * Increases the [PageRef.accessed] counter and updates [PageRef.lastAccess]
         */
        fun touch() {
            this.lastAccess = System.currentTimeMillis()
            this.accessed += 1
        }

        /**
         * Releases this [PageRef]. Using a [Resource] after releasing it is unsafe and can cause problems.
         */
        fun release() {
            synchronized(this@BufferPool.pins[this.pointer]) {
                this@BufferPool.pins[this.pointer] -= 1
            }
        }
    }
}