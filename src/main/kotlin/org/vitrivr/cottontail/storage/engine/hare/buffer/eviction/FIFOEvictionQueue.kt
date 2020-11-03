package org.vitrivr.cottontail.storage.engine.hare.buffer.eviction

import it.unimi.dsi.fastutil.PriorityQueue
import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool

/**
 * The simplest of all the [EvictionQueue]s. Evicts pages in a FIFO manner without explicit
 * prioritisation. This is a good fit e.g. for linear table scans.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FIFOEvictionQueue(size: Int) : AbstractEvictionQueue<FIFOEvictionToken>() {
    override val queue: PriorityQueue<BufferPool.PageReference> = ObjectArrayFIFOQueue(size)
    override fun token(): FIFOEvictionToken = FIFOEvictionToken
}

/**
 * Single instance [EvictionQueueToken]: Does not have any state and touching it has no effects.
 * All [FIFOEvictionToken] tokens are equal.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FIFOEvictionToken : EvictionQueueToken {
    override fun touch() {}
    override fun compareTo(other: EvictionQueueToken): Int = 0
}