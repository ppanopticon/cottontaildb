package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.type.BooleanType
import ch.unibas.dmi.dbis.cottontail.model.type.Type

inline class BooleanValue(override val value: Boolean) : Value<Boolean> {
    override val type: Type<Boolean>
        get() = BooleanType

    override val numeric: Boolean
        get() = true

    override fun compareTo(other: Value<*>): Int = when (other) {
        is BooleanValue -> if (this.value) { 1 } else { 0 }.compareTo(if (other.value) { 1.toByte() } else { 0.toByte() })
        is ByteValue -> if (this.value) { 1 } else { 0 }.compareTo(other.value)
        is ShortValue -> if (this.value) { 1 } else { 0 }.compareTo(other.value)
        is IntValue -> if (this.value) { 1 } else { 0 }.compareTo(other.value)
        is LongValue -> if (this.value) { 1 } else { 0 }.compareTo(other.value)
        is DoubleValue -> if (this.value) { 1 } else { 0 }.compareTo(other.value)
        is FloatValue -> if (this.value) { 1 } else { 0 }.compareTo(other.value)
        else -> throw IllegalArgumentException("BooleanValues can only be compared to other numeric values.")
    }
}