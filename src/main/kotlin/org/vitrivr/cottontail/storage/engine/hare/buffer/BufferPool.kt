package org.vitrivr.cottontail.storage.engine.hare.buffer

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.jmx.JmxConfig
import io.micrometer.jmx.JmxMeterRegistry
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.PageRef
import org.vitrivr.cottontail.storage.engine.hare.basics.ReferenceCounted
import org.vitrivr.cottontail.storage.engine.hare.basics.Resource
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionPolicy
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionQueue
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionQueueToken
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.memory.MemoryManager
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.shared
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock

/**
 * A [BufferPool] mediates access to a HARE file through a [DirectHareDiskManager] and facilitates
 * reading and writing [HarePage]s from/to memory and swapping [HarePage]s into the in-memory buffer.
 *
 * @see EvictionQueue
 *
 * @version 1.2.1
 * @author Ralph Gasser
 */
class BufferPool(val disk: HareDiskManager, val size: Int = 25, val evictionPolicy: EvictionPolicy) : Resource {

    companion object {
        val METER_REGISTRY: MeterRegistry = JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM)
        val PAGE_MISS_COUNTER = METER_REGISTRY.counter("Cottontail.Hare.BufferPool.PageMiss")
        val PAGE_ACCESS_COUNTER = METER_REGISTRY.counter("Cottontail.Hare.BufferPool.PageAccess")
    }

    /** Creates a new [MemoryManager.MemoryBlock] for this [BufferPool]. */
    private val memory = MemoryManager.request(this.size shl this.disk.pageShift)

    /** Array of [HarePage]s that are kept in memory. */
    private val pages = this.memory.paged(this.disk.pageShift, this.size)

    /** The internal directory that maps [PageId]s to [PageReference]s.*/
    private val pageDirectory = Long2ObjectOpenHashMap<PageReference>()

    /** [EvictionQueue] that keeps track of [PageReference] that can be reused. */
    private val evictionQueue = evictionPolicy.evictionQueue(this.size)

    /** An internal lock that mediates access to the [BufferPool.pageDirectory]. */
    private val directoryLock = StampedLock()

    /** A [ReentrantReadWriteLock] that mediates access to the closed state of this [BufferPool]. */
    private val closeLock = StampedLock()

    /** Internal flag used to indicate whether this [BufferPool] has been closed. */
    private var closed: Boolean = false

    /** Return true if this [HareDiskManager] and thus this [BufferPool] is still open. */
    override val isOpen: Boolean
        get() = this.disk.isOpen && !this.closed

    /** Physical size of the HARE page file underpinning this [BufferPool]. */
    val diskSize
        get() = this.disk.size

    /** The amount of memory used by this [BufferPool] to buffer [PageReference]s. */
    val memorySize = this.memory.size

    /** Returns the total number of buffered [HarePage]s. */
    val bufferedPages
        get() = this.pageDirectory.size

    /** Returns the total number of [HarePage]s stored in the HARE Page file underpinning this [BufferPool]. */
    val totalPages
        get() = this.disk.pages

    init {
        this.pages.forEach { this.evictionQueue.offerCandidate(PageReference(-1, Priority.LOW, it)) }
    }

    /**
     * Reads the [HarePage] identified by the given [PageId]. If a [PageReference] for the requested [HarePage]
     * exists in the [BufferPool], that [PageReference] is returned. Otherwise, the [HarePage] is read from
     * underlying storage.
     *
     * @param pageId The [PageId] of the requested [HarePage]
     * @param priority A [Priority] hint for the new [PageReference]. Acts as a hint to the [EvictionQueue].
     * @return [PageReference] for the requested [HarePage]
     */
    fun get(pageId: PageId, priority: Priority = Priority.DEFAULT): PageReference = this.closeLock.read {
        check(this.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        PAGE_ACCESS_COUNTER.increment()
        var directoryStamp = this.directoryLock.readLock()  /* Acquire non-exclusive lock to close lock.  */
        try {
            return this.pageDirectory.getOrElse(pageId) {
                PAGE_MISS_COUNTER.increment()
                directoryStamp = this.directoryLock.tryConvertToWriteLock(directoryStamp) /* Upgrade to exclusive lock */

                /* Detach new PageRef. */
                val newRef = evictPage(pageId, priority)

                /* Now read page from disk. */
                this.disk.read(pageId, newRef)

                /* Update page directory and queue and return new PageRef. */
                this.pageDirectory[pageId] = newRef
                newRef
            }.retain()
        } finally {
            this.directoryLock.unlock(directoryStamp)
        }
    }

    /**
     * Adds a range of [PageId] to this [BufferPool]'s prefetch queue.
     *
     * @param range [LongRange] that should be pre-fetched.
     */
    @Suppress("UNCHECKED_CAST")
    fun prefetch(range: LongRange) {
        check(range.count() <= this.size) { "Number of elements to prefetch is larger than BufferPool's size." }
        this@BufferPool.directoryLock.write {
            val pageRefs = range.map {
                this@BufferPool.evictPage(it, Priority.DEFAULT)
            }
            this@BufferPool.disk.read(range.first, (pageRefs.toTypedArray() as Array<HarePage>))
            pageRefs.forEach {
                this@BufferPool.pageDirectory[it.id] = it
            }
        }
    }

    /**
     * Appends a new HARE page to the page file managed by this [BufferPool] and returns a [PageId] for that page.
     *
     * @return [PageRef] for the appended [HarePage]
     */
    fun append(): PageId = this.closeLock.read {
        check(this.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        return this.disk.allocate()
    }

    /**
     * Flushes all dirty [PageRef]s to disk and resets their dirty flag. This method should be used with care, since it
     * will cause all [HarePage]s to be written to disk.
     */
    fun flush() = this.closeLock.read {
        check(this.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.shared {
            for (p in this.pageDirectory.values) {
                if (p.dirty) {
                    this.disk.update(p.id, p)
                }
            }
        }
    }

    /**
     * Synchronizes all dirty [PageRef]s with the version on disk thus resetting their dirty flag. This method should be
     * used with care, since it will cause all [HarePage]s to be read from disk.
     */
    fun synchronize() = this.closeLock.read {
        check(this.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.shared {
            for (p in this.pageDirectory.values) {
                if (p.dirty) {
                    this.disk.read(p.id, p)
                }
            }
        }
    }

    /** Closes this [BufferPool] and the underlying [HareDiskManager]. */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.directoryLock.exclusive {
                val pages = this.pageDirectory.values.toList()
                pages.forEach {
                    it.dispose()
                }
            }
            this.memory.release()
            this.closed = true
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
        return PageReference(id, priority, pageRef.buffer)
    }

    /**
     * A reference to a [HarePage] held by this [BufferPool]. These references are exposed to the upper
     * layers of the storage engine and access to a [HarePage] is only possible through such a [PageReference]
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    inner class PageReference(override val id: PageId, override val priority: Priority, data: ByteBuffer): HarePage(data), PageRef {

        /** Flag indicating whether or not this [PageReference] is dirty. */
        private val _dirty = AtomicBoolean(false)
        override val dirty
            get() = this._dirty.get()

        /** Internal reference count for this [PageReference]. */
        private val _refCount = AtomicInteger()
        override val refCount: Int
            get() = this._refCount.get()

        /** Internal lock used to protect access to the this [PageReference] during eviction. */
        private val evictionLock: StampedLock = StampedLock()

        /** The [EvictionQueueToken] that is updated on access. */
        override val token: EvictionQueueToken = this@BufferPool.evictionQueue.token()

        override fun putBytes(index: Int, value: ByteArray): PageReference {
            this._dirty.set(true)
            super.putBytes(index, value)
            return this
        }

        override fun putShorts(index: Int, value: ShortArray): PageReference {
            this._dirty.set(true)
            super.putShorts(index, value)
            return this
        }

        override fun putChars(index: Int, value: CharArray): PageReference {
            this._dirty.set(true)
            super.putChars(index, value)
            return this
        }

        override fun putInts(index: Int, value: IntArray): PageReference {
            this._dirty.set(true)
            super.putInts(index, value)
            return this
        }

        override fun putLongs(index: Int, value: LongArray): PageReference {
            this._dirty.set(true)
            super.putLongs(index, value)
            return this
        }

        override fun putFloats(index: Int, value: FloatArray): PageReference {
            this._dirty.set(true)
            super.putFloats(index, value)
            return this
        }

        override fun putDoubles(index: Int, value: DoubleArray): PageReference {
            this._dirty.set(true)
            super.putDoubles(index, value)
            return this
        }

        override fun putBytes(index: Int, value: ByteBuffer): PageReference {
            this._dirty.set(true)
            super.putBytes(index, value)
            return this
        }

        override fun putByte(index: Int, value: Byte): PageReference {
            this._dirty.set(true)
            super.putByte(index, value)
            return this
        }

        override fun putShort(index: Int, value: Short): PageReference {
            this._dirty.set(true)
            super.putShort(index, value)
            return this
        }

        override fun putChar(index: Int, value: Char): PageReference {
            this._dirty.set(true)
            super.putChar(index, value)
            return this
        }

        override fun putInt(index: Int, value: Int): PageReference {
            this._dirty.set(true)
            super.putInt(index, value)
            return this
        }

        override fun putLong(index: Int, value: Long): PageReference {
            this._dirty.set(true)
            super.putLong(index, value)
            return this
        }

        override fun putFloat(index: Int, value: Float): PageReference {
            this._dirty.set(true)
            super.putFloat(index, value)
            return this
        }

        override fun putDouble(index: Int, value: Double): PageReference {
            this._dirty.set(true)
            super.putDouble(index, value)
            return this
        }

        override fun clear(): PageReference {
            this._dirty.set(true)
            super.clear()
            return this
        }

        /**
         * Retains this [PageRef] thus increasing its reference count by one.
         */
        override fun retain() = this.evictionLock.shared {
            val oldRefCount = this._refCount.getAndUpdate {
                check(it != ReferenceCounted.REF_COUNT_DISPOSED) { "PageRef $this has been disposed and cannot be accessed anymore." }
                it+1
            }
            if (oldRefCount == 0) {
                this@BufferPool.evictionQueue.removeCandidate(this)
            }
            this.token.touch()
            this
        }

        /**
         * Releases this [PageRef] thus decreasing its reference count by one.
         */
        override fun release() = this.evictionLock.shared {
            val refCount = this._refCount.updateAndGet {
                check(it != ReferenceCounted.REF_COUNT_DISPOSED) { "PageRef $this has been disposed and cannot be accessed anymore." }
                check(it > 0) { "PageRef $this has a reference count of zero and cannot be released!" }
                it-1
            }
            if (refCount == 0) {
                this@BufferPool.evictionQueue.offerCandidate(this)
            }
        }

        /**
         * Prepares this [PageRef] for eviction. This is an internal function and not meant for use
         * outside of [BufferPool].
         *
         * <strong>Important:</strong> The assumption is, that the page [BufferPool.directoryLock] is
         * locked when this method is invoked!
         *
         * @return true if [PageRef] was successfully prepared for eviction, false otherwise.
         */
        internal fun dispose(): Boolean = this.evictionLock.write {
            return if (this._refCount.compareAndSet(0, ReferenceCounted.REF_COUNT_DISPOSED)) {
                this@BufferPool.pageDirectory.remove(this.id)
                if (this.dirty && this.id != -1L) {
                    this@BufferPool.disk.update(this.id, this)
                }
                true
            } else {
                false
            }
        }
    }
}