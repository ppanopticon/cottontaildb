package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.BooleanVectorValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [ValueStatistics] implementation for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BooleanVectorValueStatistics(type: Type<BooleanVectorValue>) : ValueStatistics<BooleanVectorValue>(type) {

    /** A histogram capturing the number of true entries per component. */
    val numberOfTrueEntries: LongArray = LongArray(this.type.logicalSize)

    /** A histogram capturing the number of false entries per component. */
    val numberOfFalseEntries: LongArray
        get() = LongArray(this.type.logicalSize) {
            this.numberOfNonNullEntries - this.numberOfTrueEntries[it]
        }

    /**
     * Updates this [DoubleVectorValueStatistics] with an inserted [DoubleVectorValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: BooleanVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                if (d) this.numberOfTrueEntries[i] = this.numberOfTrueEntries[i] + 1
            }
        }
    }

    /**
     * Updates this [DoubleVectorValueStatistics] with a deleted [DoubleVectorValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: BooleanVectorValue?) {
        super.delete(deleted)
        if (deleted != null) {
            for ((i, d) in deleted.data.withIndex()) {
                if (d) this.numberOfTrueEntries[i] = this.numberOfTrueEntries[i] - 1
            }
        }
    }

    /**
     * A [org.mapdb.Serializer] implementation for a [DoubleVectorValueStatistics] object.
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    class Serializer(val type: Type<BooleanVectorValue>) : org.mapdb.Serializer<BooleanVectorValueStatistics> {
        override fun serialize(out: DataOutput2, value: BooleanVectorValueStatistics) {
            value.numberOfTrueEntries.forEach { out.writeLong(it) }
        }

        override fun deserialize(input: DataInput2, available: Int): BooleanVectorValueStatistics {
            val stat = BooleanVectorValueStatistics(this.type)
            repeat(this.type.logicalSize) { stat.numberOfTrueEntries[it] = input.readLong() }
            return stat
        }
    }
}