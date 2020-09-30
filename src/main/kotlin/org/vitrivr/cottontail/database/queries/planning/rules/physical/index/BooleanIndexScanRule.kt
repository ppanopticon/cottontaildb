package org.vitrivr.cottontail.database.queries.planning.rules.physical.index

import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.IndexScanPhysicalNodeExpression

/**
 * A [RewriteRule] that implements a [FilterLogicalNodeExpression] preceded by a
 * [EntityScanLogicalNodeExpression] through a single [IndexScanPhysicalNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object BooleanIndexScanRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is FilterLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is FilterLogicalNodeExpression) {
            val parent = (node.copyWithInputs() as FilterLogicalNodeExpression).input
                    ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(node, "Expected parent but none was found.")
            if (parent is EntityScanLogicalNodeExpression) {
                val candidate = parent.entity.allIndexes().find {
                    it.canProcess(node.predicate)
                }
                if (candidate != null) {
                    val p = IndexScanPhysicalNodeExpression(parent.entity, candidate, node.predicate)
                    node.copyOutput()?.addInput(p)
                    return p
                }
            }
        }
        return null
    }
}