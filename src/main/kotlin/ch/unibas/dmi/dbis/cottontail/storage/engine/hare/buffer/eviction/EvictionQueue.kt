package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.eviction

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool

/**
 * An [EvictionQueue] is used by a [BufferPool] to determine, which [BufferPool.PageReference] should be
 * evicted and re-used next.  This data structure keeps track of the [BufferPool.PageReference]s that are
 * eligible for eviction and returns them to a caller (usually a [BufferPool]) upon request.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
interface EvictionQueue {
    /**
     * Polls this [EvictionQueue] for a [BufferPool.PageReference] that can be reused.
     *
     * @return [BufferPool.PageReference] ready for re-use.
     */
    fun poll(): BufferPool.PageReference

    /**
     * Enqueues a [BufferPool.PageReference] for later re-use by  the [BufferPool].
     *
     * @param ref [BufferPool.PageReference] that should be re-used
     */
    fun enqueue(ref: BufferPool.PageReference)

    /**
     * Removes a [BufferPool.PageReference] from this [EvictionQueue]. This is to prevent the
     * re-use of [BufferPool.PageReference]s that have been released but retained again at a
     * later stage.
     *
     * @param ref [BufferPool.PageReference] that should be removed
     */
    fun remove(ref: BufferPool.PageReference)
}