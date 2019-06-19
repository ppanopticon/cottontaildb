package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.type.BooleanArrayType
import ch.unibas.dmi.dbis.cottontail.model.type.Type

inline class BooleanArrayValue(override val value: BooleanArray) : Value<BooleanArray> {
    override val type: Type<BooleanArray>
        get() = BooleanArrayType

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("BooleanArrayValue can can only be compared for equality.")
    }
}