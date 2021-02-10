package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.LimitLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that implements a [LimitLogicalOperatorNode] by a [LimitPhysicalOperatorNode].
 *
 * This is a simple 1:1 replacement (implementation).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object LimitImplementationRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is LimitLogicalOperatorNode
    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is LimitLogicalOperatorNode) {
            val parent = (node.deepCopy() as LimitLogicalOperatorNode).input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            val p = LimitPhysicalOperatorNode(node.limit, node.skip)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}