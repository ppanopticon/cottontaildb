package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.TupleIdOutOfBoundException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.ReadableCursor.Companion.BYTE_CURSOR_BOF

/**
 * A [ReadableCursor] is a read-only data structure that allows for navigation in and inspection of
 * HARE data  structures such as columns through a cursor-like interface.
 *
 * Position of the [ReadableCursor]  starts at [BYTE_CURSOR_BOF] and can be changed through primitives
 * such as [ReadableCursor.next], [ReadableCursor.previous] and [ReadableCursor.tupleId]. Data at the
 * given position can then be accessed through [ReadableCursor.get], [ReadableCursor.isDeleted] and
 * [ReadableCursor.isNull]
 *
 * Due to their nature, [ReadableCursor]'s can also act as [Iterator]'s for the underlying [TupleId]s.
 * However, the [ReadableCursor] interface is much more powerful than that of a simple [Iterator].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface ReadableCursor<T : Value> : AutoCloseable, Iterator<TupleId> {

    companion object {
        const val BYTE_CURSOR_BOF = -1L
    }

    /** Maximum [TupleId] that can be reached through this [ReadableCursor]. */
    val maximum: TupleId

    /**
     * Returns the [TupleId] this [ReadableCursor] is currently pointing to.
     *
     * @return Current value for [TupleId]
     */
    fun tupleId(): TupleId

    /**
     * Sets this [ReadableCursor]'s [TupleId] to the given [TupleId].
     *
     * @param new The new, desired [TupleId] position.
     * @return New value for [TupleId]
     * @throws [TupleIdOutOfBoundException] If [TupleId] is out of bounds (i.e. > [ReadableCursor.maximum] or < [BYTE_CURSOR_BOF]).
     */
    fun tupleId(new: TupleId): TupleId

    /**
     * Moves this [ReadableCursor]'s [TupleId] to the next position.
     *
     * @return New [TupleId]
     * @throws [TupleIdOutOfBoundException] If next [TupleId] is out of bounds (i.e. > [ReadableCursor.maximum]).
     */
    override fun next(): TupleId

    /**
     * Checks if this [ReadableCursor] has a valid [TupleId] beyond the current [TupleId]. If this method
     * returns true, then the next call to [ReadableCursor.next] is guaranteed to be safe.
     *
     * @return True if there exists a valid [TupleId] beyond the current one, false otherwise.
     */
    override fun hasNext(): Boolean

    /**
     * Moves this [ReadableCursor]'s [TupleId] to the previous position.
     *
     * @return New [TupleId]
     * @throws [TupleIdOutOfBoundException] If previous [TupleId] is out of bounds (i.e. < [BYTE_CURSOR_BOF]).
     */
    fun previous(): TupleId

    /**
     * Returns a boolean indicating whether the entry the the current [ReadableCursor]'s position is null.
     *
     * @return true if the entry at the current position of the [ReadableCursor] is null and false otherwise.
     */
    fun isNull() : Boolean

    /**
     * Returns a boolean indicating whether the entry the the current [ReadableCursor]'s position has been deleted.
     *
     * @return true if the entry at the current position of the [ReadableCursor] has been deleted and false otherwise.
     */
    fun isDeleted() : Boolean

    /**
     * Returns the entry at the current [ReadableCursor] position.
     *
     * @return Entry at the current [ReadableCursor] position.
     */
    fun get() : T?
}