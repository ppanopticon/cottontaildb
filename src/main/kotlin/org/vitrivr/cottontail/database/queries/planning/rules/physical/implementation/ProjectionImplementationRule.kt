package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.ProjectionLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.ProjectionPhysicalNodeExpression

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object ProjectionImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is ProjectionLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is ProjectionLogicalNodeExpression) {
            val parent = (node.deepCopy() as ProjectionLogicalNodeExpression).input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            val p = ProjectionPhysicalNodeExpression(node.type, node.fields)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}