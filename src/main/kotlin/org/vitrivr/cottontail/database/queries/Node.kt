package org.vitrivr.cottontail.database.queries

import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Node] is an object in the tree-like structure of a query plan, be it logical, physical or operational.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface Node {
    /**
     * Performs late value binding using the given [BindingContext]. Value binding is the act of
     * replacing a [Binding], which is a placeholder for a something, by the intended content.
     * This is an in-place operation!
     *
     * Used for caching and re-use of query plans.
     *
     * @param ctx [BindingContext] to use to resolve this [Binding]s.
     * @return This [Node].
     */
    fun bindValues(ctx: BindingContext<Value>): Node

    /**
     * Calculates and returns the digest for this [Node]. The digest is similar
     * to [hashCode] and follows similar semantics: If two [Node]s are considered equal
     * in the eye of any query execution component, it should return the same digest.
     *
     * Used for caching and re-use of query plans.
     *
     * @return Digest of this [Node] as [Long]
     */
    fun digest(): Long
}