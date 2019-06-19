package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.type.Type
import ch.unibas.dmi.dbis.cottontail.model.type.StringType

inline class StringValue(override val value: String) : Value<String> {
    override val type: Type<String>
        get() = StringType

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int = when (other) {
        is StringValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("StringValues can only be compared to other StringValues.")
    }
}