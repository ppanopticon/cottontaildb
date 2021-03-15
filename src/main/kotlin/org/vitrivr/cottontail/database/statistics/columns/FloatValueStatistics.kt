package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.FloatValue
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.Float.max
import java.lang.Float.min

/**
 * A [ValueStatistics] implementation for [FloatValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatValueStatistics : ValueStatistics<FloatValue>(Type.Float) {

    /**
     * Serializer for [FloatValueStatistics].
     */
    companion object Serializer : org.mapdb.Serializer<FloatValueStatistics> {
        override fun serialize(out: DataOutput2, value: FloatValueStatistics) {
            out.writeFloat(value.min)
            out.writeFloat(value.max)
            out.writeFloat(value.sum)
        }

        override fun deserialize(input: DataInput2, available: Int): FloatValueStatistics {
            val stat = FloatValueStatistics()
            stat.min = input.readFloat()
            stat.max = input.readFloat()
            stat.sum = input.readFloat()
            return stat
        }
    }

    /** Minimum value in this [FloatValueStatistics]. */
    var min: Float = Float.MAX_VALUE

    /** Minimum value in this [FloatValueStatistics]. */
    var max: Float = Float.MIN_VALUE

    /** Sum of all floats values in this [FloatValueStatistics]. */
    var sum: Float = 0.0f

    /** The arithmetic mean for the values seen by this [FloatValueStatistics]. */
    val mean: Float
        get() = (this.sum / this.numberOfNonNullEntries)

    /**
     * Updates this [FloatValueStatistics] with an inserted [FloatValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: FloatValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = min(inserted.value, this.min)
            this.max = max(inserted.value, this.max)
            this.sum += inserted.value
        }
    }

    /**
     * Updates this [FloatValueStatistics] with a deleted [FloatValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: FloatValue?) {
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