package ch.unibas.dmi.dbis.cottontail.database.events

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.model.basics.Record

/**
 * An internal [DataChangeEvent] to signal changes made to an [Entity].
 *
 * @version 1.0
 * @author Ralph Gasser
 */
data class DataChangeEvent(val entity: Entity, val old: Record?, val new: Record?) {
    init {
        if (this.old != null && this.new != null) {
            assert(this.old.tupleId == this.new.tupleId)
        }
    }

    /** The [DataChangeEventType] of this [DataChangeEvent]. */
    val type: DataChangeEventType
        get() = when {
            old != null && new == null -> DataChangeEventType.DELETE
            old == null && new != null -> DataChangeEventType.INSERT
            old != null && new != null -> DataChangeEventType.UPDATE
            else -> DataChangeEventType.EMPTY
    }
}