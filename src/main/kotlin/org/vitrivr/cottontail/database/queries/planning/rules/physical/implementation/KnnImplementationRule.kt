package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.KnnLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.KnnPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that implements a [KnnLogicalOperatorNode] by a [KnnPhysicalOperatorNode].
 *
 * This is a simple 1:1 replacement (implementation).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object KnnImplementationRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is KnnLogicalOperatorNode
    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is KnnLogicalOperatorNode) {
            val parent = (node.deepCopy() as KnnLogicalOperatorNode).input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            val p = KnnPhysicalOperatorNode(node.predicate)
            p.addInput(parent)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}