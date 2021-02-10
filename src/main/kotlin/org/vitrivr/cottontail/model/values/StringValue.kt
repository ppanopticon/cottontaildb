package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.ScalarValue
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*

/**
 * This is a [Value] abstraction over a [String].
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
inline class StringValue(override val value: String) : ScalarValue<String> {
    companion object {

        /** */
        val EMPTY = StringValue("")

        /**
         * Generates a random [StringValue].
         *
         * @param rnd A [SplittableRandom] to generate the random numbers.
         * @return Random [StringValue]
         */
        fun random(size: Int, rnd: SplittableRandom = Value.RANDOM): StringValue {
            val builder = StringBuilder()
            rnd.ints(48, 122).filter {
                (it in 48..57 || it in 65..90 || it in 97..172)
            }.limit(size.toLong()).forEach {
                builder.appendCodePoint(it)
            }
            return StringValue(builder.toString())
        }
    }

    /** The logical size of this [StringValue]. */
    override val logicalSize: Int
        get() = this.value.length

    /** The [Type] of this [StringValue]. */
    override val type: Type<*>
        get() = Type.String

    /**
     * Compares this [StringValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [StringValue] can only be compared to other [NumericValue]s.
     *
     * @param other [Value] to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    override fun compareTo(other: Value): Int = when (other) {
        is StringValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("StringValues can only be compared to other StringValues.")
    }

    /**
     * Checks for equality between this [StringValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [StringValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is StringValue) && (other.value == this.value)
}