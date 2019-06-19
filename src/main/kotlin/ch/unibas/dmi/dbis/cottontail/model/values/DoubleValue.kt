package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.type.Type
import ch.unibas.dmi.dbis.cottontail.model.type.DoubleType

inline class DoubleValue(override val value: Double) : Value<Double> {
    override val type: Type<Double>
        get() = DoubleType

    override val numeric: Boolean
        get() = true

    override fun compareTo(other: Value<*>): Int = when (other) {
        is BooleanValue -> this.value.compareTo(if (other.value) { 1.0 } else { 0.0 })
        is ByteValue -> this.value.compareTo(other.value)
        is ShortValue -> this.value.compareTo(other.value)
        is IntValue -> this.value.compareTo(other.value)
        is LongValue -> this.value.compareTo(other.value)
        is DoubleValue -> this.value.compareTo(other.value)
        is FloatValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("DoubleValues can only be compared to other numeric values.")
    }
}