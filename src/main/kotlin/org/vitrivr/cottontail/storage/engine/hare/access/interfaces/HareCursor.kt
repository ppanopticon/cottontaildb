package org.vitrivr.cottontail.storage.engine.hare.access.interfaces

import org.vitrivr.cottontail.model.basics.CloseableIterator
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.TransactionId

/**
 * A [HareCursor] is a data structure that allows for navigation in a sequence of [TupleId]s
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
interface HareCursor<T : Value> : CloseableIterator<TupleId> {

    companion object {
        const val CURSOR_BOF: TupleId = -1L
    }

    /** The [TransactionId] of the transaction associated with this [HareColumnReader]. */
    val tid: TransactionId

    /** The [TupleId] this [HareCursor] currently points to. */
    val tupleId: TupleId

    /** Minimum [TupleId] that can be accessed through this [HareCursor]. */
    val start: TupleId

    /** Maximum [TupleId] that can be accessed through this [HareCursor]. */
    val end: TupleId
}