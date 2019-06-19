package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.type.Type
import ch.unibas.dmi.dbis.cottontail.model.type.IntArrayType

inline class IntArrayValue(override val value: IntArray) : Value<IntArray> {
    override val type: Type<IntArray>
        get() = IntArrayType

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("FloatArrayValue can can only be compared for equality.")
    }
}