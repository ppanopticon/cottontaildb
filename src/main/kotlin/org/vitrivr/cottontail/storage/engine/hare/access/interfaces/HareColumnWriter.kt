package org.vitrivr.cottontail.storage.engine.hare.access.interfaces

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.TransactionId
import org.vitrivr.cottontail.storage.engine.hare.access.EntryDeletedException
import org.vitrivr.cottontail.storage.engine.hare.access.NullValueNotAllowedException
import org.vitrivr.cottontail.storage.engine.hare.basics.Resource

/**
 * A data structure that allow for write access to a [HareColumnFile].
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
interface HareColumnWriter<T : Value> : Resource {
    /** The [TransactionId] of the transaction associated with this [HareColumnReader]. */
    val tid: TransactionId

    /**
     * Updates the [Value] for the given [TupleId].
     *
     * @param tupleId The [TupleId] to update.
     * @param value The value [T] the updated entry should contain after the update. Can be null, if the underlying data structure permits it.
     *
     * @throws EntryDeletedException If entry identified by [TupleId] has been deleted.
     * @throws NullValueNotAllowedException If [value] is null but the underlying data structure does not support null values.
     */
    fun update(tupleId: TupleId, value: T?)

    /**
     * Updates the [Value] for the given [TupleId] if, and only if, it is equal to the expected value.
     *
     * @param tupleId The [TupleId] to update.
     * @param expectedValue The value [T] the entry is expected to contain before the update. May be null.
     * @param newValue The  value [T] the updated entry should contain after the update. Can be null, if the underlying data structure permits it.
     * @return True if entry was updated, false otherwise.
     *
     * @throws EntryDeletedException If entry identified by [TupleId] has been deleted.
     * @throws NullValueNotAllowedException If [newValue] is null but the underlying data structure does not support null values.
     */
    fun compareAndUpdate(tupleId: TupleId, expectedValue: T?, newValue: T?): Boolean

    /**
     * Deletes the [Value] for the given[TupleId].
     *
     * @param tupleId The [TupleId] to delete.
     * @return The value of the entry before deletion.
     *
     * @throws EntryDeletedException If entry identified by [TupleId] has been deleted.
     */
    fun delete(tupleId: TupleId): T?

    /**
     * Appends the provided [Value] to the underlying data structure, assigning it a new [TupleId].
     *
     * @param value The value to append. Can be null, if the underlying data structure permits it.
     * @return The [TupleId] of the new value.
     *
     * @throws NullValueNotAllowedException If [value] is null but the underlying data structure does not support null values.
     */
    fun append(value: T?): TupleId

    /**
     * Commits all changes made through this [HareColumnWriter].
     */
    fun commit()

    /**
     * Performs a rollback on all changes made through this [HareColumnWriter].
     */
    fun rollback()
}