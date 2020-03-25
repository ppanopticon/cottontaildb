package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer

/**
 * A [EvictionQueue] used by a [BufferPool] to determine, which [PageRef] should be evicted next.
 * This data structure keeps track of the [BufferPool.PageReference]s that are eligible for eviction and
 * returns them to a caller (usually a [BufferPool]) upon request.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
interface EvictionQueue {

    fun poll(): BufferPool.PageReference


    fun enqueue(ref: BufferPool.PageReference)


    fun dequeue(ref: BufferPool.PageReference)

}