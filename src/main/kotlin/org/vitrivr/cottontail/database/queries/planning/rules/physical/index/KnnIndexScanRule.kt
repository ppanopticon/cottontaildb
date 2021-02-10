package org.vitrivr.cottontail.database.queries.planning.rules.physical.index

import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.KnnLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.KnnPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.FetchPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.IndexKnnPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.IndexScanPhysicalNodeExpression
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A [RewriteRule] that implements a [KnnLogicalNodeExpression] preceded by a [EntityScanLogicalNodeExpression]
 * through a single [IndexScanPhysicalNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object KnnIndexScanRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is KnnLogicalNodeExpression && node.input is EntityScanLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is KnnLogicalNodeExpression) {
            val parent = node.input
                    ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(node, "Expected parent but none was found.")
            if (parent is EntityScanLogicalNodeExpression) {
                /* Produce a candidate given the index hints. */
                val hints = node.predicate.hint
                val candidate = if (hints != null) {
                    parent.entity.allIndexes().find { it.canProcess(node.predicate) && hints.satisfies(it) }
                } else {
                    parent.entity.allIndexes().find { it.canProcess(node.predicate) }
                }

                /* Column produced by the kNN. */
                if (candidate != null) {
                    val res = when {
                        candidate.produces.contains(node.predicate.column) -> { /* Case 1: Index produces the column needed for the kNN operation. */
                            val kNN = KnnPhysicalNodeExpression(node.predicate)
                            val p = IndexKnnPhysicalNodeExpression(candidate, node.predicate)
                            kNN.addInput(p)
                        }
                        candidate.produces.contains(KnnUtilities.distanceColumnDef(node.predicate.column.name.entity())) -> { /* Case 2: Index produces distance, no kNN calculation needed. */
                            IndexKnnPhysicalNodeExpression(candidate, node.predicate)
                        }
                        else -> {  /* Case 3: Index only produces TupleIds (and potentially useless columns). Column for kNN needs to be fetched in an extra step. */
                            val kNN = KnnPhysicalNodeExpression(node.predicate)
                            val fetch = FetchPhysicalNodeExpression(
                                parent.entity,
                                arrayOf(node.predicate.column)
                            )
                            val p = IndexKnnPhysicalNodeExpression(candidate, node.predicate)
                            kNN.addInput(fetch).addInput(p)
                        }
                    }
                    val delta = parent.columns.filter { !candidate.produces.contains(it) }
                    return if (delta.isNotEmpty()) {
                        val fetch = FetchPhysicalNodeExpression(candidate.parent, delta.toTypedArray())
                        node.copyOutput()?.addInput(fetch)?.addInput(res.root)
                    } else {
                        node.copyOutput()?.addInput(res.root)
                    }
                }
            }
        }
        return null
    }
}