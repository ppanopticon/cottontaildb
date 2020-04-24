package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.eviction

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * An abstract [EvictionQueue] implementation
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class AbstractEvictionQueue : EvictionQueue {

    /** Internal [Queue] implementation used by this [AbstractEvictionQueue]. */
    protected abstract val queue: Queue<BufferPool.PageReference>

    /** */
    private val queueLock = ReentrantLock()


    /**
     * Polls this [EvictionQueue] for a [BufferPool.PageReference] that can be reused.
     *
     * @return [BufferPool.PageReference] ready for re-use.
     */
    override fun poll(): BufferPool.PageReference {
        do {
            val ref = this.queueLock.withLock {
                this.queue.poll()
            }
            if (ref != null) {
                if (ref.dispose()) {
                    return ref
                } else {
                    this.queue.offer(ref)
                }
            }
            Thread.onSpinWait()
        } while (true)
    }

    /**
     * Enqueues a [BufferPool.PageReference] for later re-use by  the [BufferPool].
     *
     * @param ref [BufferPool.PageReference] that should be re-used
     */
    @Synchronized
    override fun enqueue(ref: BufferPool.PageReference) {
        this.queueLock.withLock {
            this.queue.offer(ref)
        }
    }

    /**
     * Removes a [BufferPool.PageReference] from this [EvictionQueue]. This is to prevent the
     * re-use of [BufferPool.PageReference]s that have been released but retained again at a
     * later stage.
     *
     * @param ref [BufferPool.PageReference] that should be removed
     */
    @Synchronized
    override fun remove(ref: BufferPool.PageReference) {
        this.queueLock.withLock {
            this.queue.remove(ref)
        }
    }
}