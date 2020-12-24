package org.vitrivr.cottontail.model.values.pattern

import org.vitrivr.cottontail.model.values.StringValue

/**
 * A [PatternValue] that corresponds to a LIKE expression, i.e., uses SQL wildcards. It can either
 * be converted to a [LucenePatternValue] or matched directly through regular expressions.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LikePatternValue(value: String) : PatternValue(value) {

    companion object {
        /** Wildcard used to match zero, one or multiple characters. */
        const val ZERO_ONE_MULTIPLE_WILDCARD = "%"

        /** Wildcard used to match one character. */
        const val SINGLE_WILDCARD = "_"
    }

    /** The [Regex] expression used for matching; created lazily */
    private val regex by lazy {
        val cleaned = value.replace("\\", "\\\\")
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
                .replace(ZERO_ONE_MULTIPLE_WILDCARD, ".*") /* LIKE syntax to match multiple characters: % --> .* */
                .replace(SINGLE_WILDCARD, ".?") /* LIKE syntax to match single character: _ --> .? TODO: This can be optimized. I.e. ___ === .{3} */
        Regex(cleaned, RegexOption.DOT_MATCHES_ALL)
    }

    /**
     * Checks if the given [StringValue] matches this [LikePatternValue].
     *
     * @param value [StringValue] to match.
     * @return True on match, false otherwise.
     */
    override fun matches(value: StringValue) = this.regex.matches(value.value)

    /**
     * Converts this [LikePatternValue] to a [LucenePatternValue] and returns it.
     *
     * @return [LucenePatternValue] that corresponds to this [LikePatternValue]
     */
    fun toLucene(): LucenePatternValue {
        val cleaned = this.value
                .replace(ZERO_ONE_MULTIPLE_WILDCARD, "*") /* LIKE syntax to match multiple characters: % --> * */
                .replace(SINGLE_WILDCARD, "?") /* LIKE syntax to match single character: _ --> ? */
        return LucenePatternValue(cleaned)
    }
}