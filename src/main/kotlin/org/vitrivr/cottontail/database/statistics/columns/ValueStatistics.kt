package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.statistics.selectivity.Selectivity
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.Math.floorDiv

/**
 * A basic implementation of a [ValueStatistics] object, which is used by Cottontail DB to collect and summary statistics about
 * [Value]s it encounters.
 *
 * These classes are used to collect statistics about columns, which can then be leveraged by the query planner.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
open class ValueStatistics<T : Value>(val type: Type<T>) {

    /** Flag indicating that this [ValueStatistics] needs updating. */
    var fresh: Boolean = true
        protected set

    /** Number of null entries known to this [ValueStatistics]. */
    var numberOfNullEntries: Long = 0L
        protected set

    /** Number of non-null entries known to this [ValueStatistics]. */
    var numberOfNonNullEntries: Long = 0L
        protected set

    companion object : Serializer<ValueStatistics<*>> {
        override fun serialize(out: DataOutput2, value: ValueStatistics<*>) {
            out.packInt(value.type.ordinal)
            out.packInt(value.type.logicalSize)
            when (value) {
                is BooleanValueStatistics -> BooleanValueStatistics.serialize(out, value)
                is ByteValueStatistics -> ByteValueStatistics.serialize(out, value)
                is ShortValueStatistics -> ShortValueStatistics.serialize(out, value)
                is IntValueStatistics -> IntValueStatistics.serialize(out, value)
                is LongValueStatistics -> LongValueStatistics.serialize(out, value)
                is FloatValueStatistics -> FloatValueStatistics.serialize(out, value)
                is DoubleValueStatistics -> DoubleValueStatistics.serialize(out, value)
                is DateValueStatistics -> DateValueStatistics.serialize(out, value)
                is StringValueStatistics -> StringValueStatistics.serialize(out, value)
                is BooleanVectorValueStatistics -> BooleanVectorValueStatistics.Serializer(value.type).serialize(out, value)
                is DoubleVectorValueStatistics -> DoubleVectorValueStatistics.Serializer(value.type).serialize(out, value)
                is FloatVectorValueStatistics -> FloatVectorValueStatistics.Serializer(value.type).serialize(out, value)
                is LongVectorValueStatistics -> LongVectorValueStatistics.Serializer(value.type).serialize(out, value)
                is IntVectorValueStatistics -> IntVectorValueStatistics.Serializer(value.type).serialize(out, value)
            }
            out.writeBoolean(value.fresh)
            out.packLong(value.numberOfNullEntries)
            out.packLong(value.numberOfNonNullEntries)
        }

        override fun deserialize(input: DataInput2, available: Int): ValueStatistics<*> {
            val stat = when (val type = Type.forOrdinal(input.unpackInt(), input.unpackInt())) {
                Type.Complex32,
                Type.Complex64,
                is Type.Complex32Vector,
                is Type.Complex64Vector,
                -> ValueStatistics(type)
                Type.Boolean -> BooleanValueStatistics.deserialize(input, available)
                Type.Byte -> ByteValueStatistics.deserialize(input, available)
                Type.Double -> DoubleValueStatistics.deserialize(input, available)
                Type.Float -> FloatValueStatistics.deserialize(input, available)
                Type.Int -> IntValueStatistics.deserialize(input, available)
                Type.Long -> LongValueStatistics.deserialize(input, available)
                Type.Short -> ShortValueStatistics.deserialize(input, available)
                Type.String -> StringValueStatistics.deserialize(input, available)
                Type.Date -> DateValueStatistics.deserialize(input, available)
                is Type.BooleanVector -> BooleanVectorValueStatistics.Serializer(type).deserialize(input, available)
                is Type.DoubleVector -> DoubleVectorValueStatistics.Serializer(type).deserialize(input, available)
                is Type.FloatVector -> FloatVectorValueStatistics.Serializer(type).deserialize(input, available)
                is Type.IntVector -> IntVectorValueStatistics.Serializer(type).deserialize(input, available)
                is Type.LongVector -> LongVectorValueStatistics.Serializer(type).deserialize(input, available)
            }
            stat.fresh = input.readBoolean()
            stat.numberOfNullEntries = input.unpackLong()
            stat.numberOfNonNullEntries = input.unpackLong()
            return stat
        }
    }

    /** Total number of entries known to this [ValueStatistics]. */
    val numberOfEntries
        get() = this.numberOfNullEntries + this.numberOfNonNullEntries

    /** Smallest [Value] seen in terms of space requirement (logical size) known to this [ValueStatistics]. */
    open val minWidth: Int
        get() = this.type.logicalSize

    /** Largest [Value] in terms of space requirement (logical size) known to this [ValueStatistics] */
    open val maxWidth: Int
        get() = this.type.logicalSize

    /** Mean [Value] in terms of space requirement (logical size) known to this [ValueStatistics] */
    val avgWidth: Int
        get() = floorDiv(this.minWidth + this.maxWidth, 2)

    /**
     * Updates this [ValueStatistics] with an inserted [Value]
     *
     * @param inserted The [Value] that was deleted.
     */
    open fun insert(inserted: T?) {
        if (inserted == null) {
            this.numberOfNullEntries += 1
        } else {
            this.numberOfNonNullEntries += 1
        }
    }

    /**
     * Updates this [ValueStatistics] with a deleted [Value]
     *
     * @param deleted The [Value] that was deleted.
     */
    open fun delete(deleted: T?) {
        if (deleted == null) {
            this.numberOfNullEntries -= 1
        } else {
            this.numberOfNonNullEntries -= 1
        }
    }

    /**
     * Updates this [ValueStatistics] with a new updated value [T].
     *
     * Default implementation is simply a combination of [insert] and [delete].
     *
     * @param old The [Value] before the update.
     * @param new The [Value] after the update.
     */
    open fun update(old: T?, new: T?) {
        this.delete(old)
        this.insert(new)
    }

    /**
     * Resets this [ValueStatistics] and sets all its values to to the default value.
     */
    open fun reset() {
        this.fresh = true
        this.numberOfNullEntries = 0L
        this.numberOfNonNullEntries = 0L
    }

    /**
     * Copies this [ValueStatistics] and returns it.
     *
     * @return Copy of this [ValueStatistics].
     */
    open fun copy(): ValueStatistics<T> {
        val copy = ValueStatistics(this.type)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        return copy
    }

    /**
     * Estimates [Selectivity] of the given [BooleanPredicate.Atomic], i.e., the percentage of [Record]s that match it.
     * Defaults to [Selectivity.DEFAULT_SELECTIVITY] but can be overridden by concrete implementations.
     *
     * @param predicate [BooleanPredicate.Atomic] To estimate [Selectivity] for.
     * @return [Selectivity] estimate.
     */
    open fun estimateSelectivity(predicate: BooleanPredicate.Atomic): Selectivity = Selectivity.DEFAULT_SELECTIVITY
}