package org.vitrivr.cottontail.storage.engine.hare.buffer

import com.google.common.cache.*
import org.vitrivr.cottontail.model.basics.TransactionId
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.PageRef
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.LinkedBlockingQueue

/**
 * A [BufferPool] is basically a cache that can go in between a [HareDiskManager] and any consumer of data.
 *
 * A [BufferPool] offers means to access the [HarePage]s read from the [HareDiskManager] is a more efficient way,
 * by keeping cached references around in memory. The [BufferPool] takes care of writing changes to the [HareDiskManager]
 * when [HarePage]s get evicted.
 *
 * @see EvictionQueue
 *
 * @version 1.3.0
 * @author Ralph Gasser
 */
class BufferPool(val disk: HareDiskManager, val tid: TransactionId, val size: Int = 25) {

    /** The internal [BufferPoolCacheHandler] instance. It facilitates ore efficient access to resources. */
    private val cacheHandler = BufferPoolCacheHandler()

    /** The [LoadingCache] used by the [BufferPool]. */
    private val cache: LoadingCache<PageId, BufferPoolPageRef> = CacheBuilder
        .newBuilder()
        .maximumSize(this.size.toLong())
        .concurrencyLevel(4)
        .removalListener(this.cacheHandler)
        .build(this.cacheHandler)

    /** Returns the total number of [HarePage]s buffered by this [BufferPool]. */
    val bufferedPages
        get() = this.cache.size()

    /**
     * Reads the [HarePage] identified by the given [PageId]. If a [BufferPoolPageRef] for the requested [HarePage]
     * exists in the [BufferPool], that [BufferPoolPageRef] is returned. Otherwise, the [HarePage] is read from
     * underlying storage.
     *
     * @param pageId The [PageId] of the requested [HarePage]
     * @return [BufferPoolPageRef] for the requested [HarePage]
     */
    fun get(pageId: PageId): BufferPoolPageRef = this.cache.get(pageId)

    /**
     * Pre-fetches the [BufferPoolPageRef] for a [PageId]
     */
    fun prefetch(vararg pageId: PageId) {
        for (p in pageId) {
            this.cache.refresh(p)
        }
    }

    /**
     * Appends a new HARE page to the page file managed by this [BufferPool] and returns a [PageId] for that page.
     *
     * @return [PageRef] for the appended [HarePage]
     */
    fun append(): PageId {
        return this.disk.allocate(this.tid)
    }

    /**
     * Flushes all dirty [PageRef]s to disk and resets their dirty flag.
     *
     * This method should be used with care, since it will cause all [HarePage]s to be written to disk.
     */
    fun flush() {
        for (p in this.cache.asMap().values) {
            p.flushIfDirty()
        }
    }

    /**
     * Synchronizes all dirty [PageRef]s with the version on disk thus resetting their dirty flag.
     *
     * This method should be used with care, since it will cause all [HarePage]s to be read from disk.
     */
    fun synchronize() {
        for (p in this.cache.asMap().values) {
            p.synchronizeIfDirty()
        }
    }

    /**
     * A reference to a [HarePage] held by this [BufferPool]. These references are exposed to the upper
     * layers of the storage engine and access to a [HarePage] is only possible through such a [BufferPoolPageRef]
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    inner class BufferPoolPageRef(id: PageId, page: HarePage) : PageRef(id, page) {
        /**
         * Flushes this [BufferPoolPageRef] to disk if it has been changed and resets the dirty flag.
         */
        override fun flushIfDirty() {
            if (this._dirty.getAndSet(false)) {
                this@BufferPool.disk.update(this@BufferPool.tid, this.id, this.page)
            }
        }

        /**
         * Synchronizes this [BufferPoolPageRef] with the disk if it has been changed and resets the dirty flag.
         */
        override fun synchronizeIfDirty() {
            if (this._dirty.getAndSet(false)) {
                this@BufferPool.disk.read(this@BufferPool.tid, this.id, this.page)
            }
        }
    }


    /**
     * This is an internal [CacheLoader] and [RemovalListener] for the [BufferPool]'s cache.
     */
    private inner class BufferPoolCacheHandler : CacheLoader<PageId, BufferPoolPageRef>(), RemovalListener<PageId, BufferPoolPageRef> {

        /** Creates a new [LinkedBlockingQueue] for this [BufferPoolCacheHandler]; it can be seen as an internal cache for [HarePage]s. */
        private val queue = LinkedBlockingQueue<HarePage>()

        /** Number of allocated [ByteBuffer] instances. */
        @Volatile
        var allocated = 0
            private set

        /**
         * Loads the [BufferPoolPageRef] for the requested [PageId] from disk.
         *
         * Allocates [ByteBuffer] as needed but tries to re-use them.
         *
         * @param key [PageId] The [PageId] to load.
         * @return [BufferPoolPageRef]
         */
        override fun load(key: PageId): BufferPoolPageRef {
            var page = this.queue.poll()
            if (page == null) {
                this.allocated += 1
                page = HarePage(ByteBuffer.allocate(this@BufferPool.disk.pageSize))
            }
            val ref = BufferPoolPageRef(key, page)
            this@BufferPool.disk.read(this@BufferPool.tid, key, page)
            return ref
        }

        /**
         * Acts on removal of [BufferPoolPageRef]s. Allocates [ByteBuffer] as needed but tries to re-use them.
         *
         * @param notification [RemovalNotification] to act upon
         * @return [BufferPoolPageRef]
         */
        override fun onRemoval(notification: RemovalNotification<PageId, BufferPoolPageRef>) {
            if (notification.wasEvicted()) {
                if (notification.value.dirty) {
                    this@BufferPool.disk.update(this@BufferPool.tid, notification.key, notification.value.page)
                }
            }
            this.queue.offer(notification.value.page)
        }
    }
}