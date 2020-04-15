package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.EntryDeletedException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.NullValueNotAllowedException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.TupleId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.TupleIdOutOfBoundException

/**
 * A [WritableCursor] is a writeable proxy that allows for navigation in and editing of HARE data
 * structures such as columns. Access to entries is facilitated by [TupleId]s, that uniquely
 * identify each entry in the underlying data structure.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface WritableCursor<T: Value>  {
    /**
     * Updates the entry for the given [TupleId].
     *
     * @param tupleId The [TupleId] to update the entry for.
     * @param value The value [T] the updated entry should contain after the update. Can be null, if the underlying data structure permits it.
     *
     * @throws EntryDeletedException If entry identified by [TupleId] has been deleted.
     * @throws NullValueNotAllowedException If [value] is null but the underlying data structure does not support null values.
     * @throws TupleIdOutOfBoundException If the provided [TupleId] is out of bounds for the underlying data structure.
     */
    fun update(tupleId: TupleId, value: T?)

    /**
     * Updates the entry for the given [TupleId] if, and only if, it is equal to the expected value.
     *
     * @param tupleId The [TupleId] to update the entry for.
     * @param expectedValue The value [T] the entry is expected to contain before the update. May be null.
     * @param newValue The  value [T] the updated entry should contain after the update. Can be null, if the underlying data structure permits it.
     * @return True if entry was updated, false otherwise.
     *
     * @throws EntryDeletedException If entry identified by [TupleId] has been deleted.
     * @throws NullValueNotAllowedException If [newValue] is null but the underlying data structure does not support null values.
     * @throws TupleIdOutOfBoundException If the provided [TupleId] is out of bounds for the underlying data structure.
     */
    fun compareAndUpdate(tupleId: TupleId, expectedValue: T?, newValue: T?): Boolean

    /**
     * Deletes the entry identified by the provided [TupleId].
     *
     * @param tupleId The [TupleId] of the entry to delete
     * @return The value of the entry before deletion.
     *
     * @throws EntryDeletedException If entry identified by [TupleId] has been deleted.
     * @throws TupleIdOutOfBoundException If the provided [TupleId] is out of bounds for the underlying data structure.
     */
    fun delete(tupleId: TupleId): T?

    /**
     * Appends the provided entry to the underlying data structure, assigning it a new [TupleId].
     *
     * @param value The value to append. Can be null, if the underlying data structure permits it.
     * @return The [TupleId] of the new value.
     *
     *  @throws NullValueNotAllowedException If [value] is null but the underlying data structure does not support null values.
     */
    fun append(value: T?): TupleId
}