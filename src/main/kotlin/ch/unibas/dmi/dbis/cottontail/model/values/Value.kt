package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.type.Type


/**
 * This is an abstraction over the existing primitive types provided by Kotlin. It allows for the advanced type system implemented by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
interface Value<T : Any> {

    /** Actual value of this [Value]. */
    val value: T

    /** The [Type] of the [Value]. */
    val type: Type<T>

    /** Size of this [Value]. -1 for all types except for vector types. */
    val size
        get() = when (this.value) {
            is DoubleArray -> (value as DoubleArray).size
            is FloatArray -> (value as FloatArray).size
            is LongArray -> (value as LongArray).size
            is IntArray -> (value as IntArray).size
            is BooleanArray -> (value as BooleanArray).size
            else -> -1
        }

    /** True if this is a numeric [Value]. */
    val numeric: Boolean

    /**
     * Comparison operator between two values. Returns -1, 0 or 1 of other value is smaller, equal or greater than this value.
     *
     * @param other Value to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    operator fun compareTo(other: Value<*>): Int
}