package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.DeleteLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.DeletePhysicalNodeExpression

/**
 * A [RewriteRule] that transforms a [DeleteLogicalNodeExpression] into a [DeletePhysicalNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object DeleteImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is DeleteLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is DeleteLogicalNodeExpression) {
            val parent = node.copyWithInputs().inputs.firstOrNull()
                    ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(node, "Expected parent for DeleteLogicalNodeExpression but none was found.")
            val p = DeletePhysicalNodeExpression(node.entity)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}