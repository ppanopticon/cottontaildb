package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.eviction

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool

/**
 * An [EvictionQueueToken] is used by [EvictionQueue]s to determine, which [BufferPool.PageReference]
 * should be evicted next. Usually, that [EvictionQueueToken] captures some metric regarding a
 * [BufferPool.PageReference]'s use. What metric that is, is up to the concrete implementation.
 *
 * The decision to evict is based on that metric, usually by comparing it to other available [EvictionQueueToken]s
 * The decision to evict is based on that metric, usually by comparing it to other available [EvictionQueueToken]s
 *
 * @see EvictionQueue
 *
 * @author Ralph Gasser
 * @version 1.0
 *
 */
interface EvictionQueueToken : Comparable<EvictionQueueToken> {
    /**
     * Signals an access to the [BufferPool.PageReference] represented by this [EvictionQueueToken].
     * Updates the internal metric. Implementations must make sure, that this update takes place
     * in a thread safe manner.
     */
    fun touch()
}