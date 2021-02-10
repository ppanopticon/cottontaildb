package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.queries.Binding
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Binding] for a [Value].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ValueBinding(val index: Int, val type: Type<*>) : Binding<Value?> {
    /**
     * Returns bound [Value] for this [ValueBinding] given the [QueryContext].
     *
     * @param context [QueryContext] to use to obtain [Value] for [ValueBinding].
     * @return [Value]
     */
    override fun bind(context: QueryContext): Value? = context[this]

    override fun toString(): String = ":$index"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ValueBinding

        if (index != other.index) return false
        if (type != other.type) return false

        return true
    }

    override fun hashCode(): Int {
        var result = index
        result = 31 * result + type.hashCode()
        return result
    }
}