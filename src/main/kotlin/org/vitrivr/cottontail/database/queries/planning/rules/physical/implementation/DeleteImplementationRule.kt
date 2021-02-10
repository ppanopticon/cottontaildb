package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.DeleteLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.DeletePhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that transforms a [DeleteLogicalOperatorNode] into a [DeletePhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object DeleteImplementationRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is DeleteLogicalOperatorNode
    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is DeleteLogicalOperatorNode) {
            val parent = node.deepCopy().inputs.firstOrNull()
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent for DeleteLogicalNodeExpression but none was found."
                )
            val p = DeletePhysicalOperatorNode(node.entity)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}