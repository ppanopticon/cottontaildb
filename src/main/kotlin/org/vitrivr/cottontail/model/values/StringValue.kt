package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.ScalarValue
import org.vitrivr.cottontail.model.values.types.Value

inline class StringValue(override val value: String) : ScalarValue<String> {
    override val logicalSize: Int
        get() = value.length

    override fun compareTo(other: Value): Int = when (other) {
        is StringValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("StringValues can only be compared to other StringValues.")
    }
}