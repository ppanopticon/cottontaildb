package org.vitrivr.cottontail.storage.engine.hare.access.interfaces

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [HareCursor] is a data structure that allows for navigation in a sequence of [TupleId]s
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
interface HareCursor<T : Value>: Iterator<TupleId> {

    companion object {
        const val CURSOR_BOF: TupleId = -1L
    }

    /** The [TupleId] this [HareCursor] currently points to. */
    val tupleId: TupleId

    /** Minimum [TupleId] that can be accessed through this [HareCursor]. */
    val start: TupleId

    /** Maximum [TupleId] that can be accessed through this [HareCursor]. */
    val end: TupleId
}