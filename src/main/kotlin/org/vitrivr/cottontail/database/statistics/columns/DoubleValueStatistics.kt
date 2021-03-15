package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.Double.max
import java.lang.Double.min

/**
 * A [ValueStatistics] implementation for [DoubleValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleValueStatistics : ValueStatistics<DoubleValue>(Type.Double) {

    /**
     * Serializer for [FloatValueStatistics].
     */
    companion object Serializer : org.mapdb.Serializer<DoubleValueStatistics> {
        override fun serialize(out: DataOutput2, value: DoubleValueStatistics) {
            out.writeDouble(value.min)
            out.writeDouble(value.max)
            out.writeDouble(value.sum)
        }

        override fun deserialize(input: DataInput2, available: Int): DoubleValueStatistics {
            val stat = DoubleValueStatistics()
            stat.min = input.readDouble()
            stat.max = input.readDouble()
            stat.sum = input.readDouble()
            return stat
        }
    }

    /** Minimum value in this [DoubleValueStatistics]. */
    var min: Double = Double.MAX_VALUE

    /** Minimum value in this [DoubleValueStatistics]. */
    var max: Double = Double.MIN_VALUE

    /** Sum of all floats values in this [DoubleValueStatistics]. */
    var sum: Double = 0.0

    /**  The arithmetic mean for the values seen by this [DoubleValueStatistics]. */
    val mean: Double
        get() = (this.sum / this.numberOfNonNullEntries)

    /**
     * Updates this [FloatValueStatistics] with an inserted [DoubleValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: DoubleValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = min(inserted.value, this.min)
            this.max = max(inserted.value, this.max)
            this.sum += inserted.value
        }
    }

    /**
     * Updates this [FloatValueStatistics] with a deleted [DoubleValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: DoubleValue?) {
        super.delete(deleted)
        if (deleted != null) {
            this.sum -= deleted.value

            /* We cannot create a sensible estimate if a value is deleted. */
            if (this.min == deleted.value || this.max == deleted.value) {
                this.dirty = true
            }
        }
    }
}