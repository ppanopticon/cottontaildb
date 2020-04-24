package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.PageId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.PageRef
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.ReferenceCounted
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Resource
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.eviction.EvictionQueue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DataPage
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.memory.MemoryManager
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.read
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.shared
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.jmx.JmxConfig
import io.micrometer.jmx.JmxMeterRegistry
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock

/**
 * A [BufferPool] mediates access to a HARE file through a [DirectDiskManager] and facilitates reading and
 * writing [DataPage]s from/to memory and swapping [DataPage]s into the in-memory buffer.
 *
 * @see DirectDiskManager
 *
 * @version 1.2
 * @author Ralph Gasser
 */
class BufferPool(private val disk: DiskManager, val size: Int = 25, private val evictionQueue: EvictionQueue): Resource {

    companion object {
        val METER_REGISTRY: MeterRegistry = JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM)
        val PAGE_MISS_COUNTER = METER_REGISTRY.counter("Hare.BufferPool.PageMiss")
        val PAGE_ACCESS_COUNTER = METER_REGISTRY.counter("Hare.BufferPool.PageAccess")
    }

    /** Creates a new [MemoryManager] for this [BufferPool]. */
    private val manager = MemoryManager(this.disk.pageShift, this.size)

    /** Array of [DataPage]s that are kept in memory. */
    private val pages = Array(this.size) {
       this.manager[it]
    }

    /** The internal directory that maps [PageId]s to [PageReference]s.*/
    private val pageDirectory = Long2ObjectOpenHashMap<PageReference>()

    /** An internal lock that mediates access to the [BufferPool.pageDirectory]. */
    private val directoryLock = StampedLock()

    /** A [ReentrantReadWriteLock] that mediates access to the closed state of this [BufferPool]. */
    private val closeLock = StampedLock()

    /** Internal flag used to indicate whether this [BufferPool] has been closed. */
    private var closed: Boolean = false

    /** Return true if this [DiskManager] and thus this [BufferPool] is still open. */
    override val isOpen: Boolean
        get() = this.disk.isOpen && !this.closed

    /** Physical size of the HARE page file underpinning this [BufferPool]. */
    val diskSize
        get() = this.disk.size

    /** The amount of memory used by this [BufferPool] to buffer [PageReference]s. */
    val memorySize = this.manager.size

    /** Returns the total number of buffered [DataPage]s. */
    val bufferedPages
        get() = this.pageDirectory.size

    /** Returns the total number of [DataPage]s stored in the HARE Page file underpinning this [BufferPool]. */
    val totalPages
        get() = this.disk.pages

    init {
        this.pages.forEach { this.evictionQueue.enqueue(PageReference(-1, Priority.LOW, it)) }
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
     * Detaches a [PageReference]. Detached [PageReference]s are [PageReference]s that don't show up in the
     * [BufferPool.pageDirectory] but are still retained (i.e. have a pin count > 0). Otherwise they behave
     * as an ordinary [PageReference]. Detached [PageReference]s also count towards [BufferPool.size].
     *
     * A caller can detach a [PageReference] to use it as a buffer e.g. in combination with [BufferPool.append].
     *
     * @return A detached [PageReference].
     */
    fun detach(): PageReference = this.closeLock.read {
        check(this.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.write {
            return evictPage(-1L, Priority.LOW).retain()
        }
    }

    /**
     * Adds a range of [PageId] to this [BufferPool]'s prefetch queue.
     *
     * @param range [LongRange] that should be pre-fetched.
     */
    @Suppress("UNCHECKED_CAST")
    fun prefetch(range: LongRange) {
        check(range.count() <= this.size) { "Number of elements to prefetch is larger than BufferPool's size."}
        GlobalScope.launch {
            this@BufferPool.directoryLock.write {
                val pageRefs = range.map {
                    this@BufferPool.evictPage(it, Priority.DEFAULT)
                }
                this@BufferPool.disk.read(range.first, (pageRefs.toTypedArray() as Array<DataPage>))
                pageRefs.forEach {
                    this@BufferPool.pageDirectory[it.id] = it
                }
            }
        }
    }

    /**
     * Appends a new [PageRef] to the HARE page file managed by this [BufferPool] and returns a [PageRef] for that [PageRef]
     *
     * @param data [PageRef] containing the data that should be appended. That [PageRef] must be a detached [PageRef]
     * @return [PageRef] for the appended [DataPage]
     */
    fun append(data: PageRef): PageId = this.closeLock.read {
        check(this.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.write {
            check (data.id == -1L && data is PageReference) { "Only pages detached from this BufferPool can be appended." }
            return this.disk.allocate(data)
        }
    }

    /**
     * Flushes all dirty [PageRef]s to disk and resets their dirty flag. This method should be used
     * with care, since it will cause all [DataPage]s to be written to disk.
     */
    fun flush() = this.closeLock.read {
        check(this.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.read {
            for (p in this.pageDirectory.values) {
                if (p.dirty) {
                    this.disk.update(p.id, p)
                }
            }
        }
    }

    /**
     * Synchronizes all dirty [PageRef]s with the version on disk thus resetting their dirty flag.
     * This method should be used with care, since it will cause all [DataPage]s to be read from disk.
     */
    fun synchronize() = this.closeLock.read {
        check(this.isOpen) { "DiskManager for this HARE page file was closed and cannot be used to access data (file: ${this.disk.path})." }
        this.directoryLock.read {
            for (p in this.pageDirectory.values) {
                if (p.dirty) {
                    this.disk.read(p.id, p)
                }
            }
        }
    }

    /** Closes this [BufferPool] and the underlying [DiskManager]. */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.directoryLock.write {
                val pages = this.pageDirectory.values.toList()
                pages.forEach {
                    it.dispose()
                }
            }
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
        return PageReference(id, priority, pageRef._data)
    }

    /**
     * A reference to a [DataPage] held by this [BufferPool]. These references are exposed to the upper
     * layers of the storage engine and access to a [DataPage] is only possible through such a [PageReference]
     *
     * @author Ralph Gasser
     * @version 1.1
     */
    inner class PageReference(override val id: PageId, override val priority: Priority, data: ByteBuffer): DataPage(data), PageRef {
        /** Counter that counts how often this [PageReference] was accessed. Acts as a hint to the the [BufferPool]'s [EvictionQueue]. */
        private val _accessed = AtomicLong(0L)
        override val accessed: Long
            get() = this._accessed.get()

        /** The last access to this [PageReference]. Acts as a hint for the [BufferPool]'s [EvictionQueue]. */
        private val _lastAccess = AtomicLong(0L)
        override val lastAccess: Long
            get() = this._lastAccess.get()

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
                this@BufferPool.evictionQueue.remove(this)
            }
            this._accessed.getAndIncrement()
            this._lastAccess.lazySet(System.currentTimeMillis())
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
                this@BufferPool.evictionQueue.enqueue(this)
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