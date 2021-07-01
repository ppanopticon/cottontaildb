package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform.FetchLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers fetching of columns scanned in an [EntityScanLogicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DeferFetchOnScanRewriteRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is EntityScanLogicalOperatorNode
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is EntityScanLogicalOperatorNode) {
            val originalColumns = node.columns
            val originalGroupId = node.groupId
            var next: OperatorNode.Logical? = node.output
            while (next != null && next.groupId == originalGroupId) {
                /* Check if we encounter a node that requires specific but not all of the original columns. */
                val required = originalColumns.filter { it in next!!.requires }.toTypedArray()
                if (required.isEmpty()) {
                    next = next.output
                } else if (required.size == originalColumns.size) {
                    break
                } else {
                    val defer = originalColumns.filter { it !in required }.toTypedArray()

                    /*
                     * This is a very convoluted way of saying: We copy the tree starting from this node upwards,
                     * replace the (source) operator and introduce a FetchLogicalOperatorNode in between.
                     */
                    var p = next.copyWithInputs().base.first().output!!.copyWithOutput(EntityScanLogicalOperatorNode(originalGroupId, node.entity, required))
                    if (next.output != null) {
                        p = FetchLogicalOperatorNode(p, node.entity, defer)
                        p = next.output?.copyWithOutput(p) ?: p
                    }
                    return p
                }
            }
        }
        return null
    }
}