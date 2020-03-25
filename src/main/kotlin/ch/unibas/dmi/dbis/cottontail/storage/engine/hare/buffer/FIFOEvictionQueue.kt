package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer

import java.util.*

/**
 * The simplest of all the [EvictionQueue]s. Evicts pages in a FIFO manner without explicit priorization.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FIFOEvictionQueue(size: Int): AbstractEvictionQueue() {
    override val queue: Queue<BufferPool.PageReference> = ArrayDeque(size)
}