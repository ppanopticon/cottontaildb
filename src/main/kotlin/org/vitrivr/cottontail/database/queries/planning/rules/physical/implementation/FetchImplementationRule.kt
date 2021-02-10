package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.FetchLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that implements a [FetchLogicalOperatorNode] by a [FetchPhysicalOperatorNode].
 *
 * This is a simple 1:1 replacement (implementation).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FetchImplementationRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is FetchLogicalOperatorNode
    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is FetchLogicalOperatorNode) {
            val parent = (node.deepCopy() as FetchLogicalOperatorNode).input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            val p = FetchPhysicalOperatorNode(node.entity, node.fetch)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}