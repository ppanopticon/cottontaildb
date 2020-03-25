package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer

import java.util.*

abstract class AbstractEvictionQueue : EvictionQueue {

    abstract val queue: Queue<BufferPool.PageReference>
    override fun poll(): BufferPool.PageReference {
        do {
            synchronized(this) {
                val ref = this.queue.poll()
                if (ref?.prepareForEviction() == true) {
                    return ref
                }
            }
            Thread.onSpinWait()
        } while (true)
    }

    override fun enqueue(ref: BufferPool.PageReference) {
        check(ref.pinCount == 0) { }
        synchronized(this) {
            this.queue.offer(ref)
        }
    }

    @Synchronized
    override fun dequeue(ref: BufferPool.PageReference) {
        this.queue.remove(ref)
    }
}