package org.vitrivr.cottontail.storage.engine.hare.buffer.eviction

import it.unimi.dsi.fastutil.objects.ObjectArrayPriorityQueue
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import java.util.concurrent.atomic.AtomicLong


/**
 * An[EvictionQueue]s that evicts the [BufferPool.PageReference] first, that has been least recently
 * used i.e. the one with the smallest timestamp of the last use.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class LRUEvictionQueue(size: Int) : AbstractEvictionQueue<LRUEvictionToken>() {

    /** Priority queue that is sorted by the value of the [LRUEvictionQueue.Token] */
    override val queue = ObjectArrayPriorityQueue(size, Comparator<BufferPool.PageReference> { o1, o2 ->
        val c = o1.token.compareTo(o2.token)
        if (c == 0) {
            o1.priority.compareTo(o2.priority)
        } else {
            c
        }
    })

    /**
     * Returns a new [LRUEvictionQueue.Token]
     *
     * @return [LRUEvictionQueue.Token]
     */
    override fun token(): LRUEvictionToken = LRUEvictionToken()
}

/**
 * [EvictionQueueToken] that tracks the time of the last access.
 */
inline class LRUEvictionToken(private val _lastAccess: AtomicLong = AtomicLong(System.currentTimeMillis())) : EvictionQueueToken {
    val lastAccess
        get() = this._lastAccess.get()

    /** Updates the timestamp of the last use. */
    override fun touch() {
        this._lastAccess.set(System.currentTimeMillis())
    }

    override fun compareTo(other: EvictionQueueToken): Int {
        if (other is LRUEvictionToken) {
            return this.lastAccess.compareTo(other.lastAccess)
        } else {
            throw IllegalArgumentException("Instance of LRUEvictionToken.Token can only be compared to other instances of LRUEvictionToken.Token.")
        }
    }
}