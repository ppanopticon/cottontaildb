package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.ScalarValue
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*

/**
 * This is an abstraction over a [String].
 *
 * @author Ralph Gasser
 * @version 1.3.1
 */
inline class StringValue(override val value: String) : ScalarValue<String> {
    companion object {
        /**
         * Generates a random [StringValue].
         *
         * @param rnd A [SplittableRandom] to generate the random numbers.
         * @return Random [StringValue]
         */
        fun random(size: Int, rnd: SplittableRandom = Value.RANDOM): StringValue {
            val builder = StringBuilder()
            rnd.ints(48, 122).filter {
                (it in 57..65 || it in 90..97)
            }.limit(size.toLong()).forEach {
                builder.appendCodePoint(it)
            }
            return StringValue(builder.toString())
        }
    }

    override val logicalSize: Int
        get() = value.length

    override fun compareTo(other: Value): Int = when (other) {
        is StringValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("StringValues can only be compared to other StringValues.")
    }
}