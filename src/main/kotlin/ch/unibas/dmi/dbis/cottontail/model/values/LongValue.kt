package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.type.Type
import ch.unibas.dmi.dbis.cottontail.model.type.LongType

inline class LongValue(override val value: Long) : Value<Long> {
    override val type: Type<Long>
        get() = LongType

    override val numeric: Boolean
        get() = true

    override fun compareTo(other: Value<*>): Int = when (other) {
        is BooleanValue -> this.value.compareTo(if (other.value) { 1L } else { 0L })
        is ByteValue -> this.value.compareTo(other.value)
        is ShortValue -> this.value.compareTo(other.value)
        is IntValue -> this.value.compareTo(other.value)
        is LongValue -> this.value.compareTo(other.value)
        is DoubleValue -> this.value.compareTo(other.value)
        is FloatValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("LongValues can only be compared to other numeric values.")
    }
}