package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.eviction

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool
import java.util.*

/**
 * The simplest of all the [EvictionQueue]s. Evicts pages in a FIFO manner without explicit
 * prioritisation. This is a good fit for linear scans.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FIFOEvictionQueue(size: Int): AbstractEvictionQueue() {
    override val queue: Queue<BufferPool.PageReference> = ArrayDeque(size)
}