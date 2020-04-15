package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value

/**
 * A [Cursor] is a combination of a [ReadableCursor] and [WritableCursor]. A flag is used to
 * determine, whether the [Cursor] can be used to edit data.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Cursor<T: Value> : ReadableCursor<T>, WritableCursor<T> {
    /** True if, and only if, this [Cursor] can be used to write access. */
    val writeable: Boolean
}