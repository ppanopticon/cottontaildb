package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ConnectionOperator

/**
 * Decomposes a [FilterLogicalOperatorNode] that contains a [BooleanPredicate.Compound]
 * connected with a [ConnectionOperator.AND] into a sequence of two [FilterLogicalOperatorNode]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object RightConjunctionRewriteRule : RewriteRule {

    /**
     * Checks if this [RightConjunctionRewriteRule] can be applied to the given [OperatorNode].
     *
     * @param node The input [OperatorNode] to check.
     * @return True if [RewriteRule] can be applied, false otherwise.
     */
    override fun canBeApplied(node: OperatorNode): Boolean =
        node is FilterLogicalOperatorNode &&
                node.predicate is BooleanPredicate.Compound &&
                node.predicate.connector == ConnectionOperator.AND

    /**
     * Decomposes the provided [FilterLogicalOperatorNode] with a conjunction (AND) into two
     * consecutive [FilterLogicalOperatorNode]s, where each resulting [FilterLogicalOperatorNode]
     * covers one part of the conjunction. Gives precedence to the right part of the conjunction.
     *
     * @param node The input [OperatorNode].
     * @param ctx The [QueryContext] in which query planning takes place.
     *
     * @return The output [OperatorNode] or null, if no rewrite was done.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is FilterLogicalOperatorNode && node.predicate is BooleanPredicate.Compound && node.predicate.connector == ConnectionOperator.AND) {
            val parent = node.input?.copyWithInputs() ?: throw IllegalStateException("Encountered null node in logical operator node tree (node = $node). This is a programmer's error!")
            val ret = FilterLogicalOperatorNode(FilterLogicalOperatorNode(parent, node.predicate.p2), node.predicate.p1)
            return node.output?.copyWithOutput(ret) ?: ret
        }
        return null
    }
}