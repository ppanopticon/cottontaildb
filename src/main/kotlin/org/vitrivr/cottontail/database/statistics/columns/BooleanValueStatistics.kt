package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.BooleanValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [ValueStatistics] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BooleanValueStatistics : ValueStatistics<BooleanValue>(Type.Boolean) {

    /**
     * Serializer for [LongValueStatistics].
     */
    companion object Serializer : org.mapdb.Serializer<BooleanValueStatistics> {
        override fun serialize(out: DataOutput2, value: BooleanValueStatistics) {
            out.packLong(value.numberOfTrueEntries)
            out.packLong(value.numberOfFalseEntries)
        }

        override fun deserialize(input: DataInput2, available: Int): BooleanValueStatistics {
            val stat = BooleanValueStatistics()
            stat.numberOfTrueEntries = input.unpackLong()
            stat.numberOfFalseEntries = input.unpackLong()
            return stat
        }
    }

    /** Number of true entries for in this [BooleanValueStatistics]. */
    var numberOfTrueEntries: Long = 0L
        private set

    /** Number of false entries for in this [BooleanValueStatistics]. */
    var numberOfFalseEntries: Long = 0
        private set

    /**
     * Updates this [LongValueStatistics] with an inserted [BooleanValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: BooleanValue?) {
        when (inserted?.value) {
            null -> this.numberOfNullEntries += 1
            true -> {
                this.numberOfTrueEntries += 1
                this.numberOfNonNullEntries += 1
            }
            false -> {
                this.numberOfFalseEntries += 1
                this.numberOfNonNullEntries += 1
            }
        }
    }

    /**
     * Updates this [LongValueStatistics] with a deleted [BooleanValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: BooleanValue?) {
        when (deleted?.value) {
            null -> this.numberOfNullEntries -= 1
            true -> {
                this.numberOfTrueEntries -= 1
                this.numberOfNonNullEntries -= 1
            }
            false -> {
                this.numberOfFalseEntries -= 1
                this.numberOfNonNullEntries -= 1
            }
        }
    }
}
