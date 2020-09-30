package org.vitrivr.cottontail.storage.engine.hare.buffer.eviction

import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool

/**
 * An [EvictionQueue] is used by a [BufferPool] to determine, which [BufferPool.PageReference] should be
 * evicted and re-used next. This data structure keeps track of the [BufferPool.PageReference]s that are
 * eligible for eviction and returns them to a caller (usually a [BufferPool]) upon request.
 *
 * The prioritisation is facilitated by the means of so called [EvictionQueueToken]'s. These are objects
 * specific to the [EvictionQueue] implementation, that track certain aspects with regards to the
 * usage of the [BufferPool.PageReference] it is associated to.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
interface EvictionQueue<T : EvictionQueueToken> {
    /**
     * Polls this [EvictionQueue] for a [BufferPool.PageReference] that can be reused.
     *
     * @return [BufferPool.PageReference] ready for re-use.
     */
    fun poll(): BufferPool.PageReference

    /**
     * Adds a [BufferPool.PageReference] as candidate for later eviction and re-use.
     *
     * @param ref [BufferPool.PageReference] that should be prepared  for re-use.
     */
    fun offerCandidate(ref: BufferPool.PageReference)

    /**
     * Removes a [BufferPool.PageReference] from the list of candidates for later eviction. This is
     * to prevent the re-use of [BufferPool.PageReference]s that have been released but retained
     * again at a later stage.
     *
     * @param ref [BufferPool.PageReference] that should be removed from the list of candidates.
     */
    fun removeCandidate(ref: BufferPool.PageReference)

    /**
     * Returns a new [EvictionQueueToken].
     *
     * @return New [EvictionQueueToken]
     */
    fun token(): T
}