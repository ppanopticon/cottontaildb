package org.vitrivr.cottontail.database.queries.planning.rules.physical.index

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.exceptions.NodeExpressionTreeException
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.KnnLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.KnnPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.IndexKnnPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A [RewriteRule] that implements a [KnnLogicalOperatorNode] preceded by a [EntityScanLogicalOperatorNode]
 * through a single [IndexScanPhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object KnnIndexScanRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean =
        node is KnnLogicalOperatorNode && node.input is EntityScanLogicalOperatorNode

    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is KnnLogicalOperatorNode) {
            val parent = node.input
                ?: throw NodeExpressionTreeException.IncompleteNodeExpressionTreeException(
                    node,
                    "Expected parent but none was found."
                )
            if (parent is EntityScanLogicalOperatorNode) {
                /* Produce a candidate given the index hints. */
                val hints = node.predicate.hint
                val candidate = if (hints != null) {
                    parent.entity.allIndexes()
                        .find { it.canProcess(node.predicate) && hints.satisfies(it) }
                } else {
                    parent.entity.allIndexes().find { it.canProcess(node.predicate) }
                }

                /* Column produced by the kNN. */
                if (candidate != null) {
                    val res = when {
                        candidate.produces.contains(node.predicate.column) -> { /* Case 1: Index produces the column needed for the kNN operation. */
                            val kNN = KnnPhysicalOperatorNode(node.predicate)
                            val p = IndexKnnPhysicalOperatorNode(candidate, node.predicate)
                            kNN.addInput(p)
                        }
                        candidate.produces.contains(KnnUtilities.distanceColumnDef(node.predicate.column.name.entity())) -> { /* Case 2: Index produces distance, no kNN calculation needed. */
                            IndexKnnPhysicalOperatorNode(candidate, node.predicate)
                        }
                        else -> {  /* Case 3: Index only produces TupleIds (and potentially useless columns). Column for kNN needs to be fetched in an extra step. */
                            val kNN = KnnPhysicalOperatorNode(node.predicate)
                            val fetch = FetchPhysicalOperatorNode(
                                parent.entity,
                                arrayOf(node.predicate.column)
                            )
                            val p = IndexKnnPhysicalOperatorNode(candidate, node.predicate)
                            kNN.addInput(fetch).addInput(p)
                        }
                    }
                    val delta = parent.columns.filter { !candidate.produces.contains(it) }
                    return if (delta.isNotEmpty()) {
                        val fetch =
                            FetchPhysicalOperatorNode(candidate.parent, delta.toTypedArray())
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