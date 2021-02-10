package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.FetchLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.FetchPhysicalNodeExpression

/**
 * A [RewriteRule] that implements a [FetchLogicalNodeExpression] by a [FetchPhysicalNodeExpression].
 *
 * This is a simple 1:1 replacement (implementation).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FetchImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is FetchLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is FetchLogicalNodeExpression) {
            val parent = (node.deepCopy() as FetchLogicalNodeExpression).input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            val p = FetchPhysicalNodeExpression(node.entity, node.fetch)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}