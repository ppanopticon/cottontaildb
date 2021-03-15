package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform.FetchLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers fetching of columns that are not required.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object DeferredFetchRewriteRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = (node is EntityScanLogicalOperatorNode || node is FetchLogicalOperatorNode)
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? = when (node) {
        is EntityScanLogicalOperatorNode -> this.deferredFetchOnEntityScan(node, ctx)
        is FetchLogicalOperatorNode -> this.deferredFetchOnFetch(node, ctx)
        else -> null
    }

    /**
     * Tries to defer fetching of certain columns accessed in an [EntityScanLogicalOperatorNode] to later in the [OperatorNode] tree.
     *
     * @param node The [EntityScanLogicalOperatorNode]
     * @param ctx The [QueryContext] used for optimization.
     * @return New [OperatorNode] or null if no optimization took place.
     */
    private fun deferredFetchOnEntityScan(node: EntityScanLogicalOperatorNode, ctx: QueryContext): OperatorNode? {
        val originalColumns = node.columns
        val originalGroupId = node.groupId
        var next: OperatorNode.Logical? = node.output
        while (next != null && next.groupId == originalGroupId) {
            /** Case 1: We encounter a node that requires specific but not all of the original columns --> Defer fetching and return */
            val required = originalColumns.filter { it in next!!.requires }.toTypedArray()
            if (required.isNotEmpty() && required.size < originalColumns.size) {
                /*
                * This is a very convoluted way of saying: We copy the tree starting from this node upwards
                * and replace the (source) operator.
                */
                val defer = originalColumns.filter { it !in required }.toTypedArray()
                var p = next.copyWithInputs().base.first().output!!.copyWithOutput(EntityScanLogicalOperatorNode(originalGroupId, node.entity, required))
                p = FetchLogicalOperatorNode(p, node.entity, defer)
                p = next.output?.copyWithOutput(p) ?: p
                return p
            }

            /* Move down the tree. */
            next = next.output
        }

        return null
    }

    /**
     * Tries to defer fetching of certain columns accessed in an [EntityScanLogicalOperatorNode] to later in the [OperatorNode] tree.
     *
     * @param node The [EntityScanLogicalOperatorNode]
     * @param ctx The [QueryContext] used for optimization.
     * @return New [OperatorNode] or null if no optimization took place.
     */
    private fun deferredFetchOnFetch(node: FetchLogicalOperatorNode, ctx: QueryContext): OperatorNode? {
        val originalColumns = node.fetch
        val originalGroupId = node.groupId
        var next: OperatorNode.Logical? = node.output
        while (next != null && next.groupId == originalGroupId) {

            /** Case 1: We encounter a node that requires specific but not all of the original columns --> Defer fetching and return */
            val required = originalColumns.filter { it in next!!.requires }.toTypedArray()
            if (required.isNotEmpty() && required.size < originalColumns.size) {
                val defer = originalColumns.filter { it !in required }.toTypedArray()
                var p: OperatorNode.Logical = FetchLogicalOperatorNode(
                    node.input?.copyWithInputs() ?: throw IllegalStateException("Encountered null node in physical operator node tree (node = $node). This is a programmer's error!"),
                    node.entity,
                    required
                )
                p = next.copyWithInputs().base.first().output!!.copyWithOutput(p)
                p = FetchLogicalOperatorNode(p, node.entity, defer)
                p = next.output?.copyWithOutput(p) ?: p
                return p
            }

            /* Move down the tree. */
            next = next.output
        }

        /* Case 2: We reach end of tree and the columns fetched in the encountered [FetchLogicalOperatorNode] are not required at all --> Drop them. */
        return null
    }
}