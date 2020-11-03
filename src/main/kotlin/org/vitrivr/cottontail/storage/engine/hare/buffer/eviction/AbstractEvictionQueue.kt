package org.vitrivr.cottontail.storage.engine.hare.buffer.eviction

import it.unimi.dsi.fastutil.PriorityQueue
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * An abstract [EvictionQueue] implementation that acts as a foundation for concrete implementations.
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
abstract class AbstractEvictionQueue<T : EvictionQueueToken> : EvictionQueue<T> {

    /** Internal [Queue] implementation used by this [AbstractEvictionQueue]. */
    protected abstract val queue: PriorityQueue<BufferPool.PageReference>

    /** Internal map of all candidates that are eligible for re-use. */
    private val candidates = ObjectOpenHashSet<BufferPool.PageReference>()

    /** The internal lock used to mediate access to the [queue]. */
    private val queueLock = ReentrantLock()

    /** A [Condition] associated with this [queueLock] */
    private val condition: Condition = this.queueLock.newCondition()

    /**
     * Polls this [EvictionQueue] for a [BufferPool.PageReference] that can be reused.
     *
     * @return [BufferPool.PageReference] ready for re-use.
     */
    override fun poll(): BufferPool.PageReference {
        do {
            this.queueLock.withLock {
                if (this.queue.isEmpty) {
                    this.condition.await()
                }
                val ref = this.queue.dequeue()
                if (ref.dispose()) {
                    this.candidates.remove(ref)
                    return ref
                } else {
                    this.queue.enqueue(ref)
                }
                Thread.onSpinWait()
            }
        } while (true)
    }

    /**
     * Enqueues a [BufferPool.PageReference] for later re-use by the [BufferPool].
     *
     * @param ref [BufferPool.PageReference] that should be re-used
     */
    override fun offerCandidate(ref: BufferPool.PageReference) = this.queueLock.withLock {
        this.candidates.add(ref)
        this.queue.enqueue(ref)
        this.condition.signal()
    }

    /**
     * Removes a [BufferPool.PageReference] from this [EvictionQueue]. This is to prevent the
     * re-use of [BufferPool.PageReference]s that have been released but retained again at a
     * later stage.
     *
     * @param ref [BufferPool.PageReference] that should be removed
     */
    override fun removeCandidate(ref: BufferPool.PageReference) = this.queueLock.withLock {
        if (this.candidates.remove(ref)) {
            this.queue.clear()
            this.candidates.forEach { this.queue.enqueue(it) }
            this.condition.signal()
        }
    }
}