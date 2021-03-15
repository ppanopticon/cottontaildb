package org.vitrivr.cottontail.model.values.pattern

import org.vitrivr.cottontail.model.values.StringValue

/**
 * A [PatternValue] that corresponds to a LIKE expression, i.e., uses SQL wildcards. It can either
 * be converted to a [LucenePatternValue] or matched directly through regular expressions.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class LikePatternValue(value: String) : PatternValue(value) {

    companion object {
        /** Wildcard used to match zero, one or multiple characters. */
        const val ZERO_ONE_MULTIPLE_WILDCARD = '%'

        /** Wildcard used to match one character. */
        const val SINGLE_WILDCARD = '_'

        /**
         *
         */
        fun forValue(value: String) = when {
            value.endsWith(ZERO_ONE_MULTIPLE_WILDCARD) && value.count { it == '%' } == 1 -> StartsWith(value)
            value.startsWith(ZERO_ONE_MULTIPLE_WILDCARD) && value.count { it == '%' } == 1 -> EndsWith(value)
            else -> Regex(value)
        }
    }

    /**
     * A [LikePatternValue] for LIKE patterns that can be expressed with a regular expression.
     */
    class Regex(value: String) : LikePatternValue(value) {
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
                .replace("$ZERO_ONE_MULTIPLE_WILDCARD", ".*") /* LIKE syntax to match multiple characters: % --> .* */
                .replace("$SINGLE_WILDCARD", ".?") /* LIKE syntax to match single character: _ --> .? TODO: This can be optimized. I.e. ___ === .{3} */
            Regex(cleaned, RegexOption.DOT_MATCHES_ALL)
        }

        /**
         * Checks if the given [StringValue] matches this [LikePatternValue].
         *
         * @param value [StringValue] to match.
         * @return True on match, false otherwise.
         */
        override fun matches(value: StringValue) = this.regex.matches(value.value)
    }

    /**
     * A [LikePatternValue] for LIKE patterns that are equivalent to a 'startsWith' semantic, i.e., LIKE 'XYZ%'.
     *
     * Supposed to be faster than [LikePatternValue.Regex]
     */
    class StartsWith(value: String) : LikePatternValue(value) {

        init {
            val wildcardCount = this.value.count { it == ZERO_ONE_MULTIPLE_WILDCARD || it == SINGLE_WILDCARD }
            require(this.value[this.value.length - 1] == ZERO_ONE_MULTIPLE_WILDCARD && wildcardCount == 1) {
                "StartsWith LIKE patterns must only contain a single wildcard % at the end."
            }
        }

        /** String used for comparison. */
        val startsWith = this.value.subSequence(0, value.length - 1)

        /**
         * Checks if the given [StringValue] matches this [LikePatternValue].
         *
         * @param value [StringValue] to match.
         * @return True on match, false otherwise.
         */
        override fun matches(value: StringValue): Boolean = value.value.startsWith(this.startsWith)
    }

    /**
     * A [LikePatternValue] for LIKE patterns that are equivalent to a 'endsWith' semantic, i.e., LIKE '%XYZ'.
     *
     * Supposed to be faster than [LikePatternValue.Regex]
     */
    class EndsWith(value: String) : LikePatternValue(value) {

        init {
            val wildcardCount = this.value.count { it == ZERO_ONE_MULTIPLE_WILDCARD || it == SINGLE_WILDCARD }
            require(this.value[0] == ZERO_ONE_MULTIPLE_WILDCARD && wildcardCount == 1) { "EndsWith LIKE patterns must only contain a single wildcard % at the beginning." }
        }

        /** String used for comparison. */
        private val endsWith = this.value.substring(1, value.length - 1)

        /**
         * Checks if the given [StringValue] matches this [LikePatternValue].
         *
         * @param value [StringValue] to match.
         * @return True on match, false otherwise.
         */
        override fun matches(value: StringValue) = this.value.endsWith(this.endsWith)
    }

    /**
     * Converts this [LikePatternValue] to a [LucenePatternValue] and returns it.
     *
     * @return [LucenePatternValue] that corresponds to this [LikePatternValue]
     */
    fun toLucene(): LucenePatternValue {
        val cleaned = this.value
            .replace(ZERO_ONE_MULTIPLE_WILDCARD, '*') /* LIKE syntax to match multiple characters: % --> * */
            .replace(SINGLE_WILDCARD, '?') /* LIKE syntax to match single character: _ --> ? */
        return LucenePatternValue(cleaned)
    }
}