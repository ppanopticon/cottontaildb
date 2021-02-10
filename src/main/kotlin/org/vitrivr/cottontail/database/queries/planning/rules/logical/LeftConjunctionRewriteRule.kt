package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ConnectionOperator

/**
 * Decomposes a [FilterLogicalOperatorNode] that contains a [BooleanPredicate.Compound]
 * connected with a [ConnectionOperator.AND] into a sequence of two [FilterLogicalOperatorNode]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object LeftConjunctionRewriteRule : RewriteRule {

    /**
     * Checks if this [LeftConjunctionRewriteRule] can be applied to the given [OperatorNode].
     *
     * @param node The input [OperatorNode] to check.
     * @return True if [RewriteRule] can be applied, false otherwise.
     */
    override fun canBeApplied(node: OperatorNode): Boolean =
        node is FilterLogicalOperatorNode &&
                node.predicate is BooleanPredicate.Compound &&
                node.predicate.connector == ConnectionOperator.AND


    /**
     *  Decomposes the provided [FilterLogicalOperatorNode] with a conjunction into two consecutive
     * [FilterLogicalOperatorNode]s, where each resulting [FilterLogicalOperatorNode] covers
     * one part of the conjunction. Gives precedence to the left part of the conjunction.
     *
     * @param node The input [OperatorNode].
     * @return The output [OperatorNode] or null, if no rewrite was done.
     */
    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is FilterLogicalOperatorNode &&
            node.predicate is BooleanPredicate.Compound &&
            node.predicate.connector == ConnectionOperator.AND
        ) {

            val parent = (node.deepCopy() as FilterLogicalOperatorNode).input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            val p1 = FilterLogicalOperatorNode(node.predicate.p1)
            val p2 = FilterLogicalOperatorNode(node.predicate.p2)

            /* Connect parents of node with p1. */
            p1.addInput(parent)

            /* Connect parents with p1 with p2. */
            p2.addInput(p1)

            /* Connect p2 with children of node. */
            node.copyOutput()?.addInput(p2)

            return p1
        }
        return null
    }
}