package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.LongVectorValue
import org.vitrivr.cottontail.model.values.types.Value

import java.lang.Long.max
import java.lang.Long.min

/**
 * A [ValueStatistics] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class LongVectorValueStatistics(type: Type<LongVectorValue>) : ValueStatistics<LongVectorValue>(type) {
    /** Minimum value in this [LongVectorValueStatistics]. */
    val min: LongVectorValue = LongVectorValue(LongArray(this.type.logicalSize) { Long.MAX_VALUE })

    /** Minimum value in this [LongVectorValueStatistics]. */
    val max: LongVectorValue = LongVectorValue(LongArray(this.type.logicalSize) { Long.MIN_VALUE })

    /**
     * Updates this [LongVectorValueStatistics] with an inserted [LongVectorValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: LongVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                this.min.data[i] = min(d, this.min.data[i])
                this.max.data[i] = max(d, this.max.data[i])
            }
        }
    }

    /**
     * Updates this [LongVectorValueStatistics] with a deleted [LongVectorValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: LongVectorValue?) {
        super.delete(deleted)
        if (deleted != null) {
            for ((i, d) in deleted.data.withIndex()) {
                if (this.min.data[i] == d || this.max.data[i] == d) {
                    this.fresh = false
                }
            }
        }
    }

    /**
     * A [org.mapdb.Serializer] implementation for a [LongVectorValueStatistics] object.
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    class Serializer(private val type: Type<LongVectorValue>) : org.mapdb.Serializer<LongVectorValueStatistics> {
        override fun serialize(out: DataOutput2, value: LongVectorValueStatistics) {
            value.min.data.forEach { out.writeLong(it) }
            value.max.data.forEach { out.writeLong(it) }
        }

        override fun deserialize(input: DataInput2, available: Int): LongVectorValueStatistics {
            val stat = LongVectorValueStatistics(this.type)
            repeat(this.type.logicalSize) { stat.min.data[it] = input.readLong() }
            repeat(this.type.logicalSize) { stat.max.data[it] = input.readLong() }
            return stat
        }
    }

    /**
     * Resets this [LongVectorValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Long.MAX_VALUE
            this.max.data[i] = Long.MIN_VALUE
        }
    }

    /**
     * Copies this [LongVectorValueStatistics] and returns it.
     *
     * @return Copy of this [LongVectorValueStatistics].
     */
    override fun copy(): LongVectorValueStatistics {
        val copy = LongVectorValueStatistics(this.type)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        for (i in 0 until this.type.logicalSize) {
            copy.min.data[i] = this.min.data[i]
            copy.max.data[i] = this.max.data[i]
        }
        return copy
    }
}