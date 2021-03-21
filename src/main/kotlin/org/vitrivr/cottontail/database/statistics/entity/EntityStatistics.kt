package org.vitrivr.cottontail.database.statistics.entity

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import kotlin.math.max

/**
 * A data object that collects statistics for an entity.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class EntityStatistics(var count: Long = 0L, var maximumTupleId: TupleId = 0L) : RecordStatistics() {

    /**
     * Serializer for [EntityStatistics] object.
     */
    companion object Serializer : org.mapdb.Serializer<EntityStatistics> {
        override fun serialize(out: DataOutput2, value: EntityStatistics) {
            out.packLong(value.count)
            out.packLong(value.maximumTupleId)
            out.packInt(value.columns.size)
            value.columns.forEach { (t, u) ->
                ColumnDef.serialize(out, t)
                ValueStatistics.serialize(out, u)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): EntityStatistics {
            val statistics = EntityStatistics(input.unpackLong(), input.unpackLong())
            repeat(input.unpackInt()) {
                statistics.columns[ColumnDef.deserialize(input, available)] = ValueStatistics.deserialize(input, available) as ValueStatistics<Value>
            }
            return statistics
        }
    }

    /**
     * Consumes a [DataChangeEvent] and updates the [ValueStatistics] in this [EntityStatistics].
     *
     * @param event The [DataChangeEvent] to process
     */
    fun consume(event: DataChangeEvent) = when (event) {
        is DataChangeEvent.DeleteDataChangeEvent -> {
            this.count -= 1
            event.deleted.forEach { (t, u) -> this.columns[t]?.delete(u) }
        }
        is DataChangeEvent.InsertDataChangeEvent -> {
            this.count += 1
            this.maximumTupleId = max(this.maximumTupleId, event.tupleId)
            event.inserts.forEach { (t, u) -> this.columns[t]?.insert(u) }
        }
        is DataChangeEvent.UpdateDataChangeEvent -> {
            event.updates.forEach { (t, u) -> this.columns[t]?.update(u.first, u.second) }
        }
    }

    /**
     * Resets this [EntityStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.count = 0
        this.maximumTupleId = -1
    }


    /**
     * Creates an exact copy of this [EntityStatistics].
     *
     * @return Copy of this [EntityStatistics].
     */
    override fun copy(): EntityStatistics {
        val copy = EntityStatistics(this.count, this.maximumTupleId)
        for ((t, u) in this.columns) {
            copy[t] = u.copy()
        }
        return copy
    }
}