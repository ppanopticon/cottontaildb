package org.vitrivr.cottontail.storage.engine.hare.buffer.eviction

/**
 * Enumeration of all [EvictionPolicy]'s supported by Cottontail DB HARE.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
enum class EvictionPolicy {
    FIFO, LRU;

    /**
     * Creates and returns a new instance of [EvictionQueue]
     *
     * @param bufferSize Expected size of the [BufferPool] the [EvictionQueue] is going to be used with.
     */
    fun evictionQueue(bufferSize: Int) = when (this) {
        FIFO -> FIFOEvictionQueue(bufferSize)
        LRU -> LRUEvictionQueue(bufferSize)
    }
}