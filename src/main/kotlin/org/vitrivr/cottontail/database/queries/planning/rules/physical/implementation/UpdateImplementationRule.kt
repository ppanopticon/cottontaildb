package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.UpdateLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.UpdatePhysicalNodeExpression

/**
 * A [RewriteRule] that transforms a [UpdateLogicalNodeExpression] to a [UpdatePhysicalNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object UpdateImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is UpdateLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is UpdateLogicalNodeExpression) {
            val parent = node.copyWithInputs().inputs.firstOrNull()
                    ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(node, "Expected parent for UpdateLogicalNodeExpression but none was found.")
            val p = UpdatePhysicalNodeExpression(node.entity, node.values)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}