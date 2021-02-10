package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator

/**
 * A [RewriteRule] that implements a [FilterLogicalOperatorNode] by a [FilterPhysicalOperatorNode].
 *
 * This is a simple 1:1 replacement (implementation).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FilterImplementationRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is FilterLogicalOperatorNode
    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is FilterLogicalOperatorNode) {
            /* Check if the predicate contains MATCH operators, which can only be handled by a Lucene index. */
            if (node.predicate.atomics.any { it.operator === ComparisonOperator.MATCH }) {
                return null
            }

            val parent = (node.deepCopy() as FilterLogicalOperatorNode).input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            val p = FilterPhysicalOperatorNode(node.predicate)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}