package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.ScalarValue
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*

/**
 * This is an abstraction over a [Boolean].
 *
 * @author Ralph Gasser
 * @version 1.3.1
 */
inline class BooleanValue(override val value: Boolean): ScalarValue<Boolean> {

    companion object {
        val TRUE = BooleanValue(true)
        val FALSE = BooleanValue(false)

        /**
         * Generates a random [BooleanValue].
         *
         * @param rnd A [SplittableRandom] to generate the random numbers.
         * @return Random [BooleanValue]
         */
        fun random(rnd: SplittableRandom = Value.RANDOM) = BooleanValue(rnd.nextBoolean())
    }

    override val logicalSize: Int
        get() = -1

    override fun compareTo(other: Value): Int = when (other) {
        is BooleanValue -> when {
            this.value == other.value -> 0
            this.value -> 1
            else -> -1
        }
        else -> throw IllegalArgumentException("BooleanValue can only be compared to other BooleanValue values.")
    }
}