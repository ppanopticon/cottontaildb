package org.vitrivr.cottontail.database.queries.planning.rules.physical.index

import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that implements a [FilterLogicalOperatorNode] preceded by a
 * [EntityScanLogicalOperatorNode] through a single [IndexScanPhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.2.1
 */
object BooleanIndexScanRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean =
        node is FilterPhysicalOperatorNode && node.input is EntityScanPhysicalOperatorNode

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is FilterPhysicalOperatorNode) {
            val parent = node.input
            if (parent is EntityScanPhysicalOperatorNode) {
                val indexes = parent.entity.listIndexes()
                val candidate = indexes.find { it.canProcess(node.predicate) }
                if (candidate != null) {
                    var p: OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, ctx.txn.getTx(candidate) as IndexTx, node.predicate)
                    val delta = parent.columns.filter { !candidate.produces.contains(it) }
                    if (delta.isNotEmpty()) {
                        p = FetchPhysicalOperatorNode(p, parent.entity, delta.toTypedArray())
                    }
                    return node.output?.copyWithOutput(p) ?: p
                }
            }
        }
        return null
    }
}