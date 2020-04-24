package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.TupleId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.EntryDeletedException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.TupleIdOutOfBoundException

/**
 * A [ReadableCursor] is a readable proxy that allows for navigation in and inspection of
 * HARE data structures such as columns. Access to entries is facilitated by [TupleId]s, that
 * uniquely identify each entry in the underlying file.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface ReadableCursor<T : Value> : AutoCloseable {

    /** Maximum [TupleId] that can be accessed through this [ReadableCursor]. */
    val maximum: TupleId

    /**
     * Returns a boolean indicating whether the entry identified by the provided [TupleId] is null.
     *
     * @param tupleId The [TupleId] to check.
     * @return true if the entry for the given [TupleId] is null and false otherwise.
     *
     * @throws TupleIdOutOfBoundException If the provided [TupleId] is out of bounds for the underlying data structure.
     */
    fun isNull(tupleId: TupleId) : Boolean

    /**
     * Returns a boolean indicating whether the entry identified by the provided [TupleId] has been deleted.
     *
     * @param tupleId The [TupleId] to check.
     * @return true if the entry for the given [TupleId] has been deleted and false otherwise.
     *
     * @throws TupleIdOutOfBoundException If the provided [TupleId] is out of bounds for the underlying data structure.
     */
    fun isDeleted(tupleId: TupleId) : Boolean

    /**
     * Returns the entry at the current [ReadableCursor] position.
     *
     * @return Entry at the current [ReadableCursor] position.
     *
     * @throws EntryDeletedException If entry identified by [TupleId] has been deleted.
     * @throws TupleIdOutOfBoundException If the provided [TupleId] is out of bounds for the underlying data structure.
     */
    fun get(tupleId: TupleId) : T?

    /**
     * Iterates over the given range of [TupleId]s and executes the provided [action] for each entry.
     *
     * @param start The start [TupleId] for the iteration. Defaults to 0
     * @param end The end [TupleId] for the iteration. Defaults to [FixedHareCursor.maximum]
     * @param action The action that consumes the [TupleId] and the actual value
     */
    fun forEach(start: TupleId = 0L, end: TupleId = this.maximum, action: (TupleId, T?) -> Unit)

    /**
     * Iterates over the given range of [TupleId]s and executes the provided mapping [action] for each entry.
     *
     * @param start The start [TupleId] for the iteration. Defaults to 0
     * @param end The end [TupleId] for the iteration. Defaults to [FixedHareCursor.maximum]
     * @param action The action that consumes the [TupleId] and the actual value
     */
    fun <R> map(start: TupleId = 0L, end: TupleId = this.maximum, action: (TupleId, T?) -> R?): Collection<R?>
}