package org.vitrivr.cottontail.storage.engine.hare.access.cursor

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.EntryDeletedException

/**
 * A [ReadableCursor] is a [Cursor] that allows for navigation in and inspection of HARE data structures
 * such as columns. Access to entries is facilitated by [TupleId]s, that uniquely identify each entry
 * in the underlying file.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface ReadableCursor<T : Value> : Cursor<T> {
    /**
     * Returns a boolean indicating whether the entry this [ReadableCursor] is currently pointing to is null.
     *
     * @return true if the entry for the given [TupleId] is null and false otherwise.
     */
    fun isNull(): Boolean

    /**
     * Returns a boolean indicating whether the entry this [ReadableCursor] is currently pointing to is null.
     *
     * @return true if the entry for the given [TupleId] is null and false otherwise.
     */
    fun isDeleted(): Boolean

    /**
     * Returns the entry at the current [ReadableCursor] position.
     *
     * @return Entry at the current [ReadableCursor] position.
     *
     * @throws EntryDeletedException If entry this [ReadableCursor] is pointing to has been deleted.
     */
    fun get(): T?
}