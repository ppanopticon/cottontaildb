package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.ScalarValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [PatternValue] value as used in LIKE queries. Only used during query execution; cannot be stored!
 *
 * TODO: Unify handling of patterns once VBS is over.
 *
 * @author Ralph Gasser
 * @param 1.0
 */
class PatternValue(override val value: String) : ScalarValue<String> {

    /**
     * Returns a [Regex] representation of this [PatternValue].
     */
    val regex by lazy {
        val converted = this@PatternValue.value.replace("\\", "\\\\")
                .replace("[", "\\[") /* Escape. */
                .replace("]", "\\]") /* Escape. */
                .replace("(", "\\(") /* Escape. */
                .replace(")", "\\)") /* Escape. */
                .replace("{", "\\{") /* Escape. */
                .replace("}", "\\}") /* Escape. */
                .replace("^", "\\^") /* Escape. */
                .replace("$", "\\$") /* Escape. */
                .replace(".", "\\.") /* Escape. */
                .replace("?", "\\?") /* Escape. */
                .replace("+", "\\+") /* Escape. */
                .replace("-", "\\-") /* Escape. */
                .replace("|", "\\|") /* Escape. */
                .replace("*", ".*") /* Like *. */
                .replace("_", ".{1})") /* Like _. */
        Regex(converted)
    }

    /**
     * Returns a value representation of this [PatternValue] that can be understood by Apache Lucene.
     */
    val lucene by lazy {
        this@PatternValue.value
    }

    override val logicalSize: Int
        get() = 0

    override fun compareTo(other: Value): Int {
        throw IllegalArgumentException("PatternValue cannot be compared to other values.")
    }
}
