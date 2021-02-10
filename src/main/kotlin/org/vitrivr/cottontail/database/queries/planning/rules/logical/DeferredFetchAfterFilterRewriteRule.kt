package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.FetchLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers fetching of columns that are not required to satisfy a [FetchLogicalOperatorNode]
 * until after that [FetchLogicalOperatorNode] has been executed.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DeferredFetchAfterFilterRewriteRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean =
        (node is FilterLogicalOperatorNode && node.output !is FetchLogicalOperatorNode)

    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is FilterLogicalOperatorNode && node.output !is FetchLogicalOperatorNode) {
            val parent = node.input
            when (parent) { /* ToDo: Chain application in case of multiple filtering steps. */
                is EntityScanLogicalOperatorNode -> {
                    val scan = EntityScanLogicalOperatorNode(
                        parent.entity,
                        node.predicate.columns.toTypedArray()
                    )
                    val filter = node.copy()
                    val fetch = FetchLogicalOperatorNode(
                        parent.entity,
                        (parent.columns.filter { !node.predicate.columns.contains(it) }
                            .toTypedArray())
                    )
                    val output = node.copyOutput()
                    filter.addInput(scan)
                    fetch.addInput(filter)
                    return output!!.addInput(fetch)
                }
            }
        }
        return null
    }
}