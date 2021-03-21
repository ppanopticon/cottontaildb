package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.Long.max
import java.lang.Long.min

/**
 * A [ValueStatistics] implementation for [LongValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class LongValueStatistics : ValueStatistics<LongValue>(Type.Long) {

    /**
     * Serializer for [LongValueStatistics].
     */
    companion object Serializer : org.mapdb.Serializer<LongValueStatistics> {
        override fun serialize(out: DataOutput2, value: LongValueStatistics) {
            out.writeLong(value.min)
            out.writeLong(value.max)
        }

        override fun deserialize(input: DataInput2, available: Int): LongValueStatistics {
            val stat = LongValueStatistics()
            stat.min = input.readLong()
            stat.max = input.readLong()
            return stat
        }
    }

    /** Minimum value for this [LongValueStatistics]. */
    var min: Long = Long.MAX_VALUE

    /** Minimum value for this [LongValueStatistics]. */
    var max: Long = Long.MIN_VALUE

    /**
     * Updates this [LongValueStatistics] with an inserted [LongValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: LongValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = min(inserted.value, this.min)
            this.max = max(inserted.value, this.max)
        }
    }

    /**
     * Updates this [LongValueStatistics] with a deleted [LongValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: LongValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (this.min == deleted?.value || this.max == deleted?.value) {
            this.dirty = true
        }
    }

    /**
     * Resets this [LongValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = Long.MAX_VALUE
        this.max = Long.MIN_VALUE
    }
}