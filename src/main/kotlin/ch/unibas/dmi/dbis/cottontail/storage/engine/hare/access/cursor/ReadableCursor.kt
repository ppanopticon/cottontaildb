package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value

/**
 *
 */
interface ReadableCursor<T : Value> : AutoCloseable {


    /** Maximum [TupleId] that can be reached through this [ReadableCursor]. */
    val maximum: TupleId

    fun tupleId(): TupleId

    fun tupleId(new: TupleId)

    /**
     * Moves this [ReadableCursor] to the next position.
     */
    fun next(): TupleId

    /**
     * Moves this [ReadableCursor] to the previous position.
     */
    fun previous(): TupleId

    /**
     * Returns a boolean indicating whether the entry the the current [ReadableCursor] position is null
     *
     * @return true if the entry at the current position of the [ReadableCursor] is null and false otherwise.
     */
    fun isNull() : Boolean

    /**
     * Returns a boolean indicating whether the entry the the current [ReadableCursor] position is deleted.
     *
     * @return true if the entry at the current position of the [ReadableCursor] is null and false otherwise.
     */
    fun isDeleted() : Boolean

    /**
     * Returns the entry at the current [ReadableCursor] position.
     *
     * @return Entry at the current [ReadableCursor] position.
     */
    fun get() : T?
}