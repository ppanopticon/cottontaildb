package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [ValueStatistics] implementation for [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IntValueStatistics : ValueStatistics<IntValue>(Type.Int) {

    /**
     * Serializer for [LongValueStatistics].
     */
    companion object Serializer : org.mapdb.Serializer<IntValueStatistics> {
        override fun serialize(out: DataOutput2, value: IntValueStatistics) {
            out.writeInt(value.min)
            out.writeInt(value.max)
        }

        override fun deserialize(input: DataInput2, available: Int): IntValueStatistics {
            val stat = IntValueStatistics()
            stat.min = input.readInt()
            stat.max = input.readInt()
            return stat
        }
    }

    /** Minimum value for this [IntValueStatistics]. */
    var min: Int = Int.MAX_VALUE

    /** Minimum value for this [IntValueStatistics]. */
    var max: Int = Int.MIN_VALUE

    /**
     * Updates this [LongValueStatistics] with an inserted [IntValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: IntValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = min(inserted.value, this.min)
            this.max = max(inserted.value, this.max)
        }
    }

    /**
     * Updates this [LongValueStatistics] with a deleted [IntValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: IntValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (this.min == deleted?.value || this.max == deleted?.value) {
            this.dirty = true
        }
    }

    /**
     * Resets this [IntValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = Int.MAX_VALUE
        this.max = Int.MIN_VALUE
    }
}