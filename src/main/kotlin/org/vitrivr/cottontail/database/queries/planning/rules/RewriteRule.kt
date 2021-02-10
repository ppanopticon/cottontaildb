package org.vitrivr.cottontail.database.queries.planning.rules

import org.vitrivr.cottontail.database.queries.OperatorNode

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface RewriteRule {
    /**
     * Checks if this [LogicalRewriteRule] can be applied to the given [OperatorNode]
     *
     * @param node The input [OperatorNode] to check.
     * @return True if [LogicalRewriteRule] can be applied, false otherwise.
     */
    fun canBeApplied(node: OperatorNode): Boolean

    /**
     * Transforms the given [OperatorNode] (and potentially its parents and children) to a new
     * [OperatorNode] that produces the equivalent output.
     *
     * @param node The input [OperatorNode].
     * @return The output [OperatorNode] or null, if no rewrite was done.
     */
    fun apply(node: OperatorNode): OperatorNode?
}