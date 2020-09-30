package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.ScalarValue
import org.vitrivr.cottontail.model.values.types.Value

inline class BooleanValue(override val value: Boolean): ScalarValue<Boolean> {

    companion object {
        const val TRUE = 1.toByte()
        const val FALSE =  0.toByte()
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