package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.UpdateLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.UpdatePhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that transforms a [UpdateLogicalOperatorNode] to a [UpdatePhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object UpdateImplementationRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is UpdateLogicalOperatorNode
    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is UpdateLogicalOperatorNode) {
            val parent = node.deepCopy().inputs.firstOrNull()
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent for UpdateLogicalNodeExpression but none was found."
                )
            val p = UpdatePhysicalOperatorNode(node.entity, node.values)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}