package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.components.CompoundBooleanPredicate
import org.vitrivr.cottontail.database.queries.components.ConnectionOperator
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalNodeExpression

/**
 * Decomposes a [FilterLogicalNodeExpression] that contains a [CompoundBooleanPredicate] connected with
 * a [ConnectionOperator.AND] into a sequence of two [FilterLogicalNodeExpression]s.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object RightConjunctionRewriteRule : RewriteRule {

    /**
     * Checks if this [RightConjunctionRewriteRule] can be applied to the given [NodeExpression].
     *
     * @param node The input [NodeExpression] to check.
     * @return True if [RewriteRule] can be applied, false otherwise.
     */
    override fun canBeApplied(node: NodeExpression): Boolean =
            node is FilterLogicalNodeExpression &&
                    node.predicate is CompoundBooleanPredicate &&
                    node.predicate.connector == ConnectionOperator.AND


    /**
     * Decomposes the provided [FilterLogicalNodeExpression] with a conjunction (AND) into two
     * consecutive [FilterLogicalNodeExpression]s, where each resulting [FilterLogicalNodeExpression]
     * covers one part of the conjunction. Gives precedence to the right part of the conjunction.
     *
     * @param node The input [NodeExpression].
     * @return The output [NodeExpression] or null, if no rewrite was done.
     */
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is FilterLogicalNodeExpression &&
                node.predicate is CompoundBooleanPredicate &&
                node.predicate.connector == ConnectionOperator.AND) {

            val parent = (node.copyWithInputs() as FilterLogicalNodeExpression).input
                    ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(node, "Expected parent but none was found.")
            val p1 = FilterLogicalNodeExpression(node.predicate.p2)
            val p2 = FilterLogicalNodeExpression(node.predicate.p1)

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