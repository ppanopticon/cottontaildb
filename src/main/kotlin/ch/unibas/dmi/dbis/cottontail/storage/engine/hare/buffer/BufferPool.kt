package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer

import ch.unibas.dmi.dbis.cottontail.storage.basics.MemorySize
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.PageRef
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Releasable.Companion.PIN_COUNT_DISPOSED

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Resource
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DataPage
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId

import ch.unibas.dmi.dbis.cottontail.utilities.extensions.optimisticRead
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.read
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock

/**
 * A [BufferPool] mediates access to a HARE file through a [DirectDiskManager] and facilitates reading and
 * writing [DataPage]s from/to memory and swapping [DataPage]s into the in-memory buffer.
 *
 * @see DirectDiskManager
 *
 * @version 1.1
 * @author Ralph Gasser
 */
class BufferPool(private val disk: DiskManager, private val size: Int = 100, private val evictionQueue: EvictionQueue = FIFOEvictionQueue(size)): Resource {

    /** Allocates direct memory as [ByteBuffer] that is used to buffer [DataPage]s. This is not counted towards the heap used by the JVM! */
    private val buffer = ByteBuffer.allocateDirect(this.size * this.disk.pageSize)

    /** Array of [DataPage]s that are kept in memory. */
    private val pages = Array(this.size) {
       DataPage(this.buffer.position(it * this.disk.pageSize).limit((it+1) * this.disk.pageSize).slice())
    }

    /** The internal directory that maps [PageId]s to [PageReference]s.*/
    private val pageDirectory = Long2ObjectOpenHashMap<PageReference>()

    /** An internal lock that mediates access to the [BufferPool.pageDirectory]. */
    private val directoryLock = StampedLock()

    /** A [ReentrantReadWriteLock] that mediates access to the closed state of this [BufferPool]. */
    private val closeLock = StampedLock()

    /** Return true if this [DiskManager] and thus this [BufferPool] is still open. */
    override val isOpen: Boolean
        get() = this.disk.isOpen

    /** Physical size of the HARE page file underpinning this [BufferPool]. */
    val diskSize
        get() = this.disk.size

    /** The amount of memory used by this [BufferPool] to buffer [PageReference]s. */
    val memorySize
        get() = MemorySize((this.size * this.disk.pageSize).toDouble())

    /** Returns the total number of buffered [DataPage]s. */
    val bufferedPages
        get() = this.pageDirectory.size

    /** Returns the total number of [DataPage]s stored in the HARE Page file underpinning this [BufferPool]. */
    val totalPages
        get() = this.disk.pages

    init {
        this.pages.indices.forEach { this.evictionQueue.enqueue(PageReference(-1, Priority.LOW, it)) }
    }

    /**
     * Reads the [DataPage] identified by the given [PageId]. If a [PageReference] for the requested [DataPage]
     * exists in the [BufferPool], that [PageReference] is returned. Otherwise, the [DataPage] is read from
     * underlying storage.
     *
     * @param pageId The [PageId] of the requested [DataPage]
     * @param priority A [Priority] hint for the new [PageReference]. Acts as a hint to the [EvictionQueue].
     * @return [PageReference] for the requested [DataPage]
     */
    fun get(pageId: PageId, priority: Priority = Priority.DEFAULT): PageRef = this.closeLock.read {
        check(this.disk.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        var directoryStamp = this.directoryLock.readLock()  /* Acquire non-exclusive lock to close lock.  */
        try {
            val ref = this.pageDirectory.getOrElse(pageId) {
                directoryStamp = this.directoryLock.tryConvertToWriteLock(directoryStamp) /* Upgrade to exclusive lock */
                /* Detach new PageRef. */
                val newRef = evictPage(pageId, priority)

                /* Now read page from disk. */
                this.disk.read(pageId, newRef.page)

                /* Update page directory and queue and return new PageRef. */
                this.pageDirectory[pageId] = newRef
                newRef
            }
            return ref.retain()
        } finally {
            this.directoryLock.unlock(directoryStamp)
        }
    }

    /**
     * Appends a new [PageReference] to the HARE page file managed by this [BufferPool] and returns a [PageReference] for that [PageReference]
     *
     * @param priority A [Priority] hint for the new [PageReference]. Acts as a hint to the [EvictionQueue].
     * @param data [PageReference] containing the data that should be appended. That [PageReference] must be a detached [PageReference]
     * @return [PageReference] for the appended [DataPage]
     */
    fun append(data: PageRef): PageId = this.closeLock.read {
        check(this.disk.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.write {
            check (data.id == -1L) { }
            return this.disk.allocate(data.page)
        }
    }

    /**
     * Detaches a [PageReference]. Detached [PageReference]s are [PageReference]s that don't show up in the [BufferPool.pageDirectory]
     * but are still retained (i.e. have a pin count > 0). Otherwise they behave as an ordinary [PageReference]. Detached
     * [PageReference]s also count towards [BufferPool.size].
     *
     * A caller can detach a [PageReference] to use it as a buffer e.g. in combination with [BufferPool.append].
     *
     * @return A detached [PageReference].
     */
    fun detach(): PageRef = this.closeLock.read {
        check(this.disk.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.write {
            return evictPage(-1L, Priority.LOW).retain()
        }
    }

    /**
     * Flushes all dirty [PageReference]s to disk and resets their dirty flag. This method should be used
     * with care, since it will cause all [DataPage]s to be written to disk.
     */
    fun flush() = this.closeLock.read {
        check(this.disk.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.read {
            for (p in this.pageDirectory.values) {
                if (p.dirty) {
                    this.disk.update(p.id, p.page)
                }
            }
        }
    }

    /**
     * Synchronizes all dirty [PageReference]s with the version on disk thus resetting their dirty flag.
     * This method should be used with care, since it will cause all [DataPage]s to be read from disk.
     */
    fun synchronize() = this.closeLock.read {
        check(this.disk.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.read {
            for (p in this.pageDirectory.values) {
                if (p.dirty) {
                    this.disk.update(p.id, p.page)
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
     * Tries to find a free [PageReference] and prepares that [PageReference] for re-use by the [BufferPool].
     * This method will block, until such a [PageReference] becomes available.
     *
     * @return Index to the [PageReference] within [BufferPool.pages]
     */
    private fun evictPage(id: PageId, priority: Priority): PageReference {
        val pageRef: PageReference = this.evictionQueue.poll()
        return PageReference(id, priority, pageRef.pointer)
    }

    /**
     * A reference to a [DataPage] held by this [BufferPool]. These references are exposed to the upper
     * layers of the storage engine and access to a [DataPage] is only possible through such a [PageReference]
     *
     * @author Ralph Gasser
     * @version 1.0
     */
        inner class PageReference(override val id: PageId, override val priority: Priority, val pointer: Int): PageRef {

        /** Reference to the [DataPage] this [PageReference] is pointing to. */
        override val page: DataPage
            get() {
                check (this.pinCount != PIN_COUNT_DISPOSED) { "PageRef has already been disposed and cannot be used to access a page anymore."  }
                return this@BufferPool.pages[this.pointer]
            }

        /** Reference to the [ByteBuffer] that backs the [Page] this [PageReference] is pointing to. The returned [ByteBuffer] is readonly. */
        override val data: ByteBuffer
            get() {
                check (this.pinCount != PIN_COUNT_DISPOSED) { "PageRef has already been disposed and cannot be used to access a page anymore."  }
                return this@BufferPool.pages[this.pointer].data.asReadOnlyBuffer()
            }

        /** Counter that counts how often this [PageReference] was accessed. Acts as a hint to the the [BufferPool]'s [EvictionQueue]. */
        @Volatile
        override var accessed: Long = 0L
            private set

        /** The last access to this [PageReference]. Acts as a hint for the [BufferPool]'s [EvictionQueue]. */
        @Volatile
        override var lastAccess: Long = System.currentTimeMillis()
            private set

        /** Flag indicating whether or not this [PageReference] is dirty. */
        @Volatile
        override var dirty = false
            private set

        /** The pin count of this [PageReference]. As soon as this number drops to zero, it can potentially be evicted by the [BufferPool]. */
        @Volatile
        override var pinCount = 0
            internal set

        internal val lock: StampedLock = StampedLock()

        override fun getBytes(index: Int, byteBuffer: ByteBuffer): ByteBuffer = this.lock.optimisticRead {
            this.page.getBytes(index, byteBuffer)
        }
        override fun getBytes(index: Int, bytes: ByteArray): ByteArray = this.lock.optimisticRead { this.page.getBytes(index, bytes) }
        override fun getBytes(index: Int, limit: Int): ByteArray = this.lock.optimisticRead { this.page.getBytes(index, limit) }
        override fun getBytes(index: Int): ByteArray = this.lock.optimisticRead { this.page.getBytes(index) }
        override fun getByte(index: Int): Byte = this.lock.optimisticRead { this.page.getByte(index) }
        override fun getShort(index: Int): Short = this.lock.optimisticRead { this.page.getShort(index) }
        override fun getChar(index: Int): Char = this.lock.optimisticRead { this.page.getChar(index) }
        override fun getInt(index: Int): Int = this.lock.optimisticRead { this.page.getInt(index) }
        override fun getLong(index: Int): Long = this.lock.optimisticRead { this.page.getLong(index) }
        override fun getFloat(index: Int): Float =  this.lock.optimisticRead { this.page.getFloat(index) }
        override fun getDouble(index: Int): Double = this.lock.optimisticRead { this.page.getDouble(index) }

        override fun putBytes(index: Int, value: ByteArray): PageReference = this.lock.write {
            this.dirty = true
            this.page.putBytes(index, value)
            this
        }

        override fun putBytes(index: Int, value: ByteBuffer): PageReference = this.lock.write {
            this.dirty = true
            this.page.putBytes(index, value)
            this
        }

        override fun putByte(index: Int, value: Byte): PageReference = this.lock.write {
            this.dirty = true
            this.page.putByte(index, value)
            this
        }

        override fun putShort(index: Int, value: Short): PageReference = this.lock.write {
            this.dirty = true
            this.page.putShort(index, value)
            this
        }

        override fun putChar(index: Int, value: Char): PageReference = this.lock.write {
            this.dirty = true
            this.page.putChar(index, value)
            this
        }

        override fun putInt(index: Int, value: Int): PageReference = this.lock.write {
            this.dirty = true
            this.page.putInt(index, value)
            this
        }

        override fun putLong(index: Int, value: Long): PageReference = this.lock.write {
            this.dirty = true
            this.page.putLong(index, value)
            this
        }

        override fun putFloat(index: Int, value: Float): PageReference = this.lock.write {
            this.dirty = true
            this.page.putFloat(index, value)
            this
        }

        override fun putDouble(index: Int, value: Double): PageReference = this.lock.write {
            this.dirty = true
            this.page.putDouble(index, value)
            this
        }

        override fun clear(): PageReference {
            this.dirty = true
            this.page.clear()
            return this
        }

        /**
         * Retains this [PageReference] thus increasing its pin count by one.
         *
         * @throws IllegalStateException If the [PageReference] has already been released completely.
         */
        override fun retain(): PageReference = this.lock.write {
            check (this.pinCount > PIN_COUNT_DISPOSED) { "PageRef has already been disposed and cannot be retained again."  }
            if (this.pinCount == 0) {
                this@BufferPool.evictionQueue.dequeue(this)
            }
            this.lastAccess = System.currentTimeMillis()
            this.accessed += 1
            this.pinCount += 1
            return this
        }

        /**
         * Releases this [PageReference]. Using a [Resource] after releasing it is unsafe and can cause problems.
         *
         * @throws IllegalStateException If the [PageReference] has already been released completely.
         */
        override fun release(): PageReference = this.lock.write {
            check (this.pinCount > 0) { "PageRef has already been disposed and cannot be released again."  }
            this.pinCount -= 1
            if (this.pinCount == 0) {
                this@BufferPool.evictionQueue.enqueue(this)
            }
            return this
        }

        /**
         * Prepares this [PageRef] for eviction. This is an internal function and not meant for use outside of [BufferPool].
         *
         * @return true if [PageRef] was successfully prepared for eviction, false otherwise.
         */
        internal fun prepareForEviction(): Boolean = this.lock.write {
            if (this.pinCount == 0) {
                this@BufferPool.pageDirectory.remove(this.id)
                if (this.dirty && this.id != -1L) {
                    this@BufferPool.disk.update(this.id, this.page)
                }
                this.pinCount = PIN_COUNT_DISPOSED
                true
            } else {
                false
            }
        }
    }
}