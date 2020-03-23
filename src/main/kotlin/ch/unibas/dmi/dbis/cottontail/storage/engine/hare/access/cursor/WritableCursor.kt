package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value

/**
 *
 */
interface WritableCursor<T: Value> : ReadableCursor<T> {
    /**
     *
     */
    fun update(value: T?)

    /**
     *
     */
    fun compareAndUpdate(expected: T?, newValue: T?): Boolean

    /**
     *
     */
    fun append(value: T?)

    /**
     *
     */
    fun delete(): T?
}