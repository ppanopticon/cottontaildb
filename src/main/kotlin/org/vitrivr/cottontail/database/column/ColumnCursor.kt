package org.vitrivr.cottontail.database.column

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Iterator] implementation for [TupleId]s of a [Column] that allows for direct
 * read-through at the current position.
 *
 * This may allow for certain optimization depending on the [ColumnCursor] implementation.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface ColumnCursor<T : Value> : Iterator<TupleId> {

    /**
     * Reads the value at the current [ColumnCursor] position and returns it.
     *
     * @return The value [T] at the position of this [ColumnCursor].
     */
    fun readThrough(): T?
}