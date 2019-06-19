package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.type.Type
import ch.unibas.dmi.dbis.cottontail.model.type.LongArrayType

inline class LongArrayValue(override val value: LongArray) : Value<LongArray> {
    override val type: Type<LongArray>
        get() = LongArrayType

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("FloatArrayValue can can only be compared for equality.")
    }
}