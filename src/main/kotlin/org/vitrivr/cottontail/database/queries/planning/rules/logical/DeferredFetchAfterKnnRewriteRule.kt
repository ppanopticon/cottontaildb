package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.KnnLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.FetchLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalNodeExpression

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object DeferredFetchAfterKnnRewriteRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = (node is KnnLogicalNodeExpression && node.output !is FetchLogicalNodeExpression)
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is KnnLogicalNodeExpression && node.output !is FetchLogicalNodeExpression) {
            val parent = node.input
            when (parent) {
                is EntityScanLogicalNodeExpression -> {
                    val scan = EntityScanLogicalNodeExpression(parent.entity, arrayOf(node.predicate.column))
                    val knn = node.copy()
                    val fetch = FetchLogicalNodeExpression(parent.entity, (parent.columns.filter { it != node.predicate.column }.toTypedArray()))
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
