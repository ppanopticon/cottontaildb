package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.FetchLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalNodeExpression

/**
 * A [RewriteRule] that defers fetching of columns that are not required to satisfy a [FetchLogicalNodeExpression]
 * until after that [FetchLogicalNodeExpression] has been executed.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DeferredFetchAfterFilterRewriteRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = (node is FilterLogicalNodeExpression && node.output !is FetchLogicalNodeExpression)
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is FilterLogicalNodeExpression && node.output !is FetchLogicalNodeExpression) {
            val parent = node.input
            when (parent) { /* ToDo: Chain application in case of multiple filtering steps. */
                is EntityScanLogicalNodeExpression -> {
                    val scan = EntityScanLogicalNodeExpression(parent.entity, node.predicate.columns.toTypedArray())
                    val filter = node.copy()
                    val fetch = FetchLogicalNodeExpression(parent.entity, (parent.columns.filter { !node.predicate.columns.contains(it) }.toTypedArray()))
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