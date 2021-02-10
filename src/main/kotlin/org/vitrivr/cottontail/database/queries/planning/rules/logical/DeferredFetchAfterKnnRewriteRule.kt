package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.KnnLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.FetchLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers fetching of columns that are not required to satisfy a [KnnLogicalOperatorNode]
 * until after that [KnnLogicalOperatorNode] has been executed.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DeferredFetchAfterKnnRewriteRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean =
        (node is KnnLogicalOperatorNode && node.output !is FetchLogicalOperatorNode)

    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is KnnLogicalOperatorNode && node.output !is FetchLogicalOperatorNode) {
            val parent = node.input
            when (parent) { /* ToDo: Chain application in case of multiple filtering steps. */
                is EntityScanLogicalOperatorNode -> {
                    val scan =
                        EntityScanLogicalOperatorNode(parent.entity, arrayOf(node.predicate.column))
                    val knn = node.copy()
                    val fetch = FetchLogicalOperatorNode(
                        parent.entity,
                        (parent.columns.filter { it != node.predicate.column }.toTypedArray())
                    )
                    val output = node.copyOutput()
                    knn.addInput(scan)
                    fetch.addInput(knn)
                    return output!!.addInput(fetch)
                }
            }
        }
        return null
    }
}
