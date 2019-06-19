package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.type.Type
import ch.unibas.dmi.dbis.cottontail.model.type.DoubleArrayType

inline class DoubleArrayValue(override val value: DoubleArray) : Value<DoubleArray> {
    override val type: Type<DoubleArray>
        get() = DoubleArrayType

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("DoubleArrayValue can can only be compared for equality.")
    }
}