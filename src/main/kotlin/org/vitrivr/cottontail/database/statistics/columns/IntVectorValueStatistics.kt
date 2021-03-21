package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.IntVectorValue
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [ValueStatistics] implementation for [IntVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class IntVectorValueStatistics(type: Type<IntVectorValue>) : ValueStatistics<IntVectorValue>(type) {
    /** Minimum value in this [IntVectorValueStatistics]. */
    val min: IntVectorValue = IntVectorValue(IntArray(this.type.logicalSize) { Int.MAX_VALUE })

    /** Minimum value in this [IntVectorValueStatistics]. */
    val max: IntVectorValue = IntVectorValue(IntArray(this.type.logicalSize) { Int.MIN_VALUE })

    /**
     * Updates this [IntVectorValueStatistics] with an inserted [IntVectorValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: IntVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                this.min.data[i] = min(d, this.min.data[i])
                this.max.data[i] = max(d, this.max.data[i])
            }
        }
    }

    /**
     * Updates this [IntVectorValueStatistics] with a deleted [IntVectorValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: IntVectorValue?) {
        super.delete(deleted)
        if (deleted != null) {
            for ((i, d) in deleted.data.withIndex()) {
                if (this.min.data[i] == d || this.max.data[i] == d) {
                    this.dirty = true
                }
            }
        }
    }

    /**
     * A [org.mapdb.Serializer] implementation for a [IntVectorValueStatistics] object.
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    class Serializer(private val type: Type<IntVectorValue>) : org.mapdb.Serializer<IntVectorValueStatistics> {
        override fun serialize(out: DataOutput2, value: IntVectorValueStatistics) {
            value.min.data.forEach { out.writeInt(it) }
            value.max.data.forEach { out.writeInt(it) }
        }

        override fun deserialize(input: DataInput2, available: Int): IntVectorValueStatistics {
            val stat = IntVectorValueStatistics(this.type)
            repeat(this.type.logicalSize) { stat.min.data[it] = input.readInt() }
            repeat(this.type.logicalSize) { stat.max.data[it] = input.readInt() }
            return stat
        }
    }

    /**
     * Resets this [IntVectorValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Int.MAX_VALUE
            this.max.data[i] = Int.MIN_VALUE
        }
    }
}