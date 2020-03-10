package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer

import java.util.Comparator

/**
 * A [EvictionPolicy] used by a [BufferPool] to determine, which [PageRef] should be evicted next.
 * Basically acts a [Comparator] for [PageRef]s that returns a [PageRef] based on availability and
 * precedence.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface EvictionPolicy : Comparator<BufferPool.PageRef>