package org.vitrivr.cottontail.database.queries.planning.nodes.interfaces

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface RewriteRule {
    /**
     * Checks if this [LogicalRewriteRule] can be applied to the given [NodeExpression]
     *
     * @param node The input [NodeExpression] to check.
     * @return True if [LogicalRewriteRule] can be applied, false otherwise.
     */
    fun canBeApplied(node: NodeExpression): Boolean

    /**
     * Transforms the given [NodeExpression] (and potentially its parents and children) to a new
     * [NodeExpression] that produces the equivalent output.
     *
     * @param node The input [NodeExpression].
     * @return The output [NodeExpression] or null, if no rewrite was done.
     */
    fun apply(node: NodeExpression): NodeExpression?
}