package org.vitrivr.cottontail.storage.engine.hare.access.interfaces

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.EntryDeletedException

/**
 * A data structure that allow for read access to a [HareColumnFile].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface HareColumnReader<T: Value> {
    /**
     * Returns the entry for the given [TupleId]
     *
     * @param tupleId The [TupleId] to retrieve the entry for.
     * @return Entry for the given [TupleId] position.
     *
     * @throws EntryDeletedException If entry this [ReadableCursor] is pointing to has been deleted.
     */
    fun get(tupleId: TupleId): T?

    /**
     * Returns the number of entries for the [HareColumnFile] backing this [HareColumnReader].
     *
     * @return Number of entries in this [HareColumnFile].
     */
    fun count(): Long

    /**
     * Returns a boolean indicating whether the entry for the given [TupleId] is null.
     *
     * @param tupleId The [TupleId] to retrieve the entry for.
     * @return true if the entry for the given [TupleId] is null and false otherwise.
     */
    fun isNull(tupleId: TupleId): Boolean

    /**
     * Returns a boolean indicating whether the entry for the given [TupleId] is null.
     *
     * @return true if the entry for the given [TupleId] is null and false otherwise.
     */
    fun isDeleted(tupleId: TupleId): Boolean
}