package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.Float.max
import java.lang.Float.min

/**
 * A [ValueStatistics] implementation for [FloatVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorValueStatistics(type: Type<FloatVectorValue>) : ValueStatistics<FloatVectorValue>(type) {
    /** Minimum value in this [FloatVectorValueStatistics]. */
    val min: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize) { Float.MAX_VALUE })

    /** Minimum value in this [FloatVectorValueStatistics]. */
    val max: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize) { Float.MIN_VALUE })

    /** Sum of all floats values in this [FloatVectorValueStatistics]. */
    val sum: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize))

    /** The arithmetic for the values seen by this [DoubleVectorValueStatistics]. */
    val avg: FloatVectorValue
        get() = FloatVectorValue(FloatArray(this.type.logicalSize) {
            this.sum[it].value / this.numberOfNonNullEntries
        })

    /**
     * Updates this [FloatVectorValueStatistics] with an inserted [FloatVectorValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: FloatVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                this.min.data[i] = min(d, this.min.data[i])
                this.max.data[i] = max(d, this.max.data[i])
                this.sum.data[i] += d
            }
        }
    }

    /**
     * Updates this [FloatVectorValueStatistics] with a deleted [FloatVectorValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: FloatVectorValue?) {
        super.delete(deleted)
        if (deleted != null) {
            for ((i, d) in deleted.data.withIndex()) {
                /* We cannot create a sensible estimate if a value is deleted. */
                if (this.min.data[i] == d || this.max.data[i] == d) {
                    this.dirty = true
                }
                this.sum.data[i] -= d
            }
        }
    }

    /**
     * A [org.mapdb.Serializer] implementation for a [FloatVectorValueStatistics] object.
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    class Serializer(val type: Type<FloatVectorValue>) : org.mapdb.Serializer<FloatVectorValueStatistics> {
        override fun serialize(out: DataOutput2, value: FloatVectorValueStatistics) {
            value.min.data.forEach { out.writeFloat(it) }
            value.max.data.forEach { out.writeFloat(it) }
            value.sum.data.forEach { out.writeFloat(it) }
        }

        override fun deserialize(input: DataInput2, available: Int): FloatVectorValueStatistics {
            val stat = FloatVectorValueStatistics(this.type)
            repeat(this.type.logicalSize) { stat.min.data[it] = input.readFloat() }
            repeat(this.type.logicalSize) { stat.max.data[it] = input.readFloat() }
            repeat(this.type.logicalSize) { stat.sum.data[it] = input.readFloat() }
            return stat
        }
    }
}