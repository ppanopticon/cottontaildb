package org.vitrivr.cottontail.model.values.pattern

import org.vitrivr.cottontail.model.values.StringValue

/**
 * A [PatternValue] that corresponds to a Apache Lucene query string. Requires a Apache Lucene
 * based index for filtering, i.e., direct matching is not possible.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LucenePatternValue(value: String) : PatternValue(value) {
    /**
     * Checks if the given [StringValue] matches this [LucenePatternValue]. Always throws an [UnsupportedOperationException]
     *
     * @param value [StringValue] to match.
     * @throws [UnsupportedOperationException]
     */
    override fun matches(value: StringValue): Boolean {
        throw UnsupportedOperationException("A LucenePatternValue cannot be used to match values directly; lucene index required.")
    }
}