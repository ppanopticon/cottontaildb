package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.eviction

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool
import java.util.*
import kotlin.Comparator


/**
 *
 */
class DefaultEvictionQueue : AbstractEvictionQueue() {
    override val queue = PriorityQueue(Comparator<BufferPool.PageReference> { o1, o2 ->
        if (o1.priority == o2.priority) {
            o1.lastAccess.compareTo(o2.lastAccess)
        } else {
            o1.priority.compareTo(o2.priority)
        }
    })
}