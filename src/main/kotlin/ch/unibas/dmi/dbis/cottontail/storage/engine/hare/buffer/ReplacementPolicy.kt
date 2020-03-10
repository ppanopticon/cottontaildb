package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer

import java.util.Comparator

/**
 * A [ReplacementPolicy] used by a [BufferPool] to determine, which [PageRef] should be replaced.
 * Basically acts a [Comparator] for [PageRef]s that returns a [PageRef] based on availability and
 * precedence.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface ReplacementPolicy {
    /**
     * Returns an index to a [BufferPool.PageRef] that can be replaced with new content.
     *
     * @param pages Array of [BufferPool.PageRef] to select from.
     * @return Index of the [BufferPool.PageRef] that should be replaced.
     */
    fun next(pages: Iterable<BufferPool.PageRef>): BufferPool.PageRef
}