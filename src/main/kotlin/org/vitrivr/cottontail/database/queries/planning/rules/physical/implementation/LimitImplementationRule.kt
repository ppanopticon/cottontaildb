package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.LimitLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.LimitPhysicalNodeExpression

/**
 * A [RewriteRule] that implements a [LimitLogicalNodeExpression] by a [LimitPhysicalNodeExpression].
 *
 * This is a simple 1:1 replacement (implementation).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object LimitImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is LimitLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is LimitLogicalNodeExpression) {
            val parent = (node.deepCopy() as LimitLogicalNodeExpression).input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            val p = LimitPhysicalNodeExpression(node.limit, node.skip)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}