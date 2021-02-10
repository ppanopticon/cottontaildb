package org.vitrivr.cottontail.model.values.pattern

import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.ScalarValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * [PatternValue]s can be used to check, if the expression they contain matches a given [StringValue].
 * They can be used in LIKE and MATCH queries.
 *
 * @author Ralph Gasser
 * @param 1.2.0
 */
abstract class PatternValue(override val value: String) : ScalarValue<String> {

    /** The logical size of this [PatternValue]. */
    override val logicalSize: Int
        get() = this.value.length

    /** The [Type] of this [PatternValue]. */
    override val type: Type<*>
        get() = Type.String

    /**
     * Checks if the given [StringValue] matches this [PatternValue].
     *
     * @param value [StringValue] to match.
     * @return True on match, false otherwise.
     */
    abstract fun matches(value: StringValue): Boolean

    override fun compareTo(other: Value): Int = when (other) {
        is PatternValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("PatternValues can only be compared to other PatternValues.")
    }

    override fun isEqual(other: Value): Boolean = (other is PatternValue) && (other.value == this.value)
}
