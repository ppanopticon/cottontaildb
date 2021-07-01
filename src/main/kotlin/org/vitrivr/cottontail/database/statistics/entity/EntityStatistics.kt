package org.vitrivr.cottontail.database.statistics.entity

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import kotlin.math.max

/**
 * A data object that collects statistics for an entity.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class EntityStatistics(var count: Long = 0L, var maximumTupleId: TupleId = -1) : RecordStatistics() {

    /**
     * Serializer for [EntityStatistics] object.
     */
    companion object Serializer : org.mapdb.Serializer<EntityStatistics> {
        override fun serialize(out: DataOutput2, value: EntityStatistics) {
            out.packLong(value.count)
            out.packLong(value.maximumTupleId)
            out.packInt(value.columns.size)
            value.columns.forEach { (t, u) ->
                out.writeUTF(t.toString())
                ValueStatistics.serialize(out, u)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): EntityStatistics {
            val statistics = EntityStatistics(input.unpackLong(), input.unpackLong())
            repeat(input.unpackInt()) {
                statistics.columns[Name.ColumnName(*input.readUTF().split('.').toTypedArray())] = ValueStatistics.deserialize(input, available) as ValueStatistics<Value>
            }
            return statistics
        }
    }

    /**
     * Consumes a [Operation.DataManagementOperation] and updates the [ValueStatistics] in this [EntityStatistics].
     *
     * @param action The [Operation.DataManagementOperation] to process
     */
    fun consume(action: Operation.DataManagementOperation) = when (action) {
        is Operation.DataManagementOperation.DeleteOperation -> {
            this.count -= 1
            action.deleted.forEach { (t, u) -> this.columns[t]?.delete(u) }
        }
        is Operation.DataManagementOperation.InsertOperation -> {
            this.count += 1
            this.maximumTupleId = max(this.maximumTupleId, action.tupleId)
            action.inserts.forEach { (t, u) -> this.columns[t]?.insert(u) }
        }
        is Operation.DataManagementOperation.UpdateOperation -> {
            action.updates.forEach { (t, u) -> this.columns[t]?.update(u.first, u.second) }
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