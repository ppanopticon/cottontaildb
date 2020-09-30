package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.KnnLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.KnnPhysicalNodeExpression

/**
 * A [RewriteRule] that implements a [KnnLogicalNodeExpression] by a [KnnPhysicalNodeExpression].
 *
 * This is a simple 1:1 replacement (implementation).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object KnnImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is KnnLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is KnnLogicalNodeExpression) {
            val parent = (node.copyWithInputs() as KnnLogicalNodeExpression).input ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(node, "Expected parent but none was found.")
            val p = KnnPhysicalNodeExpression(node.predicate)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}