package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.ProjectionLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.ProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object ProjectionImplementationRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is ProjectionLogicalOperatorNode
    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is ProjectionLogicalOperatorNode) {
            val parent = (node.deepCopy() as ProjectionLogicalOperatorNode).input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            val p = ProjectionPhysicalOperatorNode(node.type, node.fields)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}