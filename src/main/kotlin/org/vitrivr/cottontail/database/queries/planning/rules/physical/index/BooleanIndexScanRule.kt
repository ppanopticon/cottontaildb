package org.vitrivr.cottontail.database.queries.planning.rules.physical.index

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that implements a [FilterLogicalOperatorNode] preceded by a
 * [EntityScanLogicalOperatorNode] through a single [IndexScanPhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
object BooleanIndexScanRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is FilterLogicalOperatorNode
            && node.input is EntityScanLogicalOperatorNode

    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is FilterLogicalOperatorNode) {
            val parent = node.input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            if (parent is EntityScanLogicalOperatorNode) {
                val candidate = parent.entity.allIndexes().find { it.canProcess(node.predicate) }
                if (candidate != null) {
                    val p = IndexScanPhysicalOperatorNode(candidate, node.predicate)
                    val delta = parent.columns.filter { !candidate.produces.contains(it) }
                    return if (delta.isNotEmpty()) {
                        val fetch =
                            FetchPhysicalOperatorNode(candidate.parent, delta.toTypedArray())
                        node.copyOutput()?.addInput(fetch)?.addInput(p.root)
                    } else {
                        node.copyOutput()?.addInput(p.root)
                    }
                }
            }
        }
        return null
    }
}