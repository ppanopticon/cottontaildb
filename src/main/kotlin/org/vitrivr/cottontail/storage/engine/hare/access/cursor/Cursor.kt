package org.vitrivr.cottontail.storage.engine.hare.access.cursor

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Cursor] is a data structure that allows for navigation in a sequence of [TupleId]s
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Cursor<T : Value> : AutoCloseable {
    /** Maximum [TupleId] that can be accessed through this [Cursor]. */
    val maximum: TupleId

    /** The [TupleId] this [Cursor] currently points to. */
    val tupleId: TupleId

    /**
     * Moves this [Cursor] to the next entry.
     *
     * @return True, if this [Cursor] has been moved. False otherwise.
     */
    fun next(): Boolean

    /**
     * Moves this [Cursor] to the next entry.
     *
     * @return True, if this [Cursor] has been moved. False otherwise.
     */
    fun previous(): Boolean

    /**
     * Moves this [Cursor] to the specified position
     *
     * @throws
     */
    fun seek(tupleId: TupleId): Boolean
}