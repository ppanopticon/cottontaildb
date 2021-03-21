package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [ValueStatistics] implementation for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DoubleVectorValueStatistics(type: Type<DoubleVectorValue>) : ValueStatistics<DoubleVectorValue>(type) {
    /** Minimum value in this [DoubleVectorValueStatistics]. */
    val min: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize) { Double.MAX_VALUE })

    /** Minimum value in this [DoubleVectorValueStatistics]. */
    val max: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize) { Double.MIN_VALUE })

    /** Sum of all floats values in this [DoubleVectorValueStatistics]. */
    val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize))

    /** The arithmetic for the values seen by this [DoubleVectorValueStatistics]. */
    val avg: DoubleVectorValue
        get() = DoubleVectorValue(DoubleArray(this.type.logicalSize) {
            this.sum[it].value / this.numberOfNonNullEntries
        })

    /**
     * Updates this [DoubleVectorValueStatistics] with an inserted [DoubleVectorValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: DoubleVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                this.min.data[i] = java.lang.Double.min(d, this.min.data[i])
                this.max.data[i] = java.lang.Double.max(d, this.max.data[i])
                this.sum.data[i] += d
            }
        }
    }

    /**
     * Updates this [DoubleVectorValueStatistics] with a deleted [DoubleVectorValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: DoubleVectorValue?) {
        super.delete(deleted)
        if (deleted != null) {
            for ((i, d) in deleted.data.withIndex()) {
                /* We cannot create a sensible estimate if a value is deleted. */
                if (this.min.data[i] == d || this.max.data[i] == d) {
                    this.fresh = false
                }
                this.sum.data[i] -= d
            }
        }
    }

    /**
     * A [org.mapdb.Serializer] implementation for a [DoubleVectorValueStatistics] object.
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    class Serializer(val type: Type<DoubleVectorValue>) : org.mapdb.Serializer<DoubleVectorValueStatistics> {
        override fun serialize(out: DataOutput2, value: DoubleVectorValueStatistics) {
            value.min.data.forEach { out.writeDouble(it) }
            value.max.data.forEach { out.writeDouble(it) }
            value.sum.data.forEach { out.writeDouble(it) }
        }

        override fun deserialize(input: DataInput2, available: Int): DoubleVectorValueStatistics {
            val stat = DoubleVectorValueStatistics(this.type)
            repeat(this.type.logicalSize) { stat.min.data[it] = input.readDouble() }
            repeat(this.type.logicalSize) { stat.max.data[it] = input.readDouble() }
            repeat(this.type.logicalSize) { stat.sum.data[it] = input.readDouble() }
            return stat
        }
    }

    /**
     * Resets this [DoubleVectorValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Double.MAX_VALUE
            this.max.data[i] = Double.MIN_VALUE
            this.sum.data[i] = 0.0
        }
    }

    /**
     * Copies this [DoubleVectorValueStatistics] and returns it.
     *
     * @return Copy of this [DoubleVectorValueStatistics].
     */
    override fun copy(): DoubleVectorValueStatistics {
        val copy = DoubleVectorValueStatistics(this.type)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        for (i in 0 until this.type.logicalSize) {
            copy.min.data[i] = this.min.data[i]
            copy.max.data[i] = this.max.data[i]
            copy.sum.data[i] = this.sum.data[i]
        }
        return copy
    }
}