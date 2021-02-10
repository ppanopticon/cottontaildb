package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.FilterPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator

/**
 * A [RewriteRule] that implements a [FilterLogicalNodeExpression] by a [FilterPhysicalNodeExpression].
 *
 * This is a simple 1:1 replacement (implementation).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FilterImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is FilterLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is FilterLogicalNodeExpression) {
            /* Check if the predicate contains MATCH operators, which can only be handled by a Lucene index. */
            if (node.predicate.atomics.any { it.operator === ComparisonOperator.MATCH }) {
                return null
            }

            val parent = (node.deepCopy() as FilterLogicalNodeExpression).input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            val p = FilterPhysicalNodeExpression(node.predicate)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}