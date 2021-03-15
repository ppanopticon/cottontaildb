package org.vitrivr.cottontail.database.events

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An internal [DataChangeEvent] to signal changes made to an [DefaultEntity].
 *
 * @version 1.0.1
 * @author Ralph Gasser
 */
sealed class DataChangeEvent(val entity: Entity, val tupleId: TupleId) {

    /**
     * A [DataChangeEvent] that signals a INSERT into an [DefaultEntity]
     */
    class InsertDataChangeEvent(
        entity: Entity,
        tupleId: TupleId,
        val inserts: Map<ColumnDef<*>, Value?>
    ) : DataChangeEvent(entity, tupleId)

    /**
     * A [DataChangeEvent] that signals an UPDATE in an [DefaultEntity]
     */
    class UpdateDataChangeEvent(
        entity: Entity,
        tupleId: TupleId,
        val updates: Map<ColumnDef<*>, Pair<Value?, Value?>>,
    ) : DataChangeEvent(entity, tupleId)

    /**
     * A [DataChangeEvent] that signals a DELETE from an [DefaultEntity]
     */
    class DeleteDataChangeEvent(
        entity: Entity,
        tupleId: TupleId,
        val deleted: Map<ColumnDef<*>, Value?>
    ) : DataChangeEvent(entity, tupleId)
}