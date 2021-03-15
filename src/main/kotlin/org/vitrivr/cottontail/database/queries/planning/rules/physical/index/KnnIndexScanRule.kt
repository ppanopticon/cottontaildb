package org.vitrivr.cottontail.database.queries.planning.rules.physical.index

import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform.DistancePhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A [RewriteRule] that implements a NNS operator constellation (scan -> distance -> sort -> limit) by an index scan.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object KnnIndexScanRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is DistancePhysicalOperatorNode

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is DistancePhysicalOperatorNode) {
            val scan = node.input
            if (scan is EntityScanPhysicalOperatorNode) {
                val sort = node.output
                if (sort is SortPhysicalOperatorNode) {
                    val limit = sort.output
                    if (limit is LimitPhysicalOperatorNode) {
                        /* Produce a candidate given the index hints. */
                        val hints = node.predicate.hint
                        val entityTx = (ctx.txn.getTx(scan.entity) as EntityTx)
                        val candidate = if (hints != null) {
                            entityTx.listIndexes().find { it.canProcess(node.predicate) && hints.satisfies(it) }
                        } else {
                            entityTx.listIndexes().find { it.canProcess(node.predicate) }
                        }

                        /* Column produced by the kNN. */
                        val column = KnnUtilities.distanceColumnDef(node.predicate.column.name.entity())
                        if (candidate != null) {
                            when {
                                /* Case 1: Index produces distance, hence no distance calculation required! */
                                candidate.produces.contains(column) -> {
                                    var p: OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, candidate, node.predicate)
                                    val delta = scan.columns.filter { !candidate.produces.contains(it) && it != node.predicate.column }
                                    if (delta.isNotEmpty()) {
                                        p = FetchPhysicalOperatorNode(p, candidate.parent, delta.toTypedArray())
                                    }
                                    return sort.output?.copyWithOutput(p) ?: p /* TODO: Index should indicate if results are sorted. */
                                }

                                /* Case 2: Index produces the columns needed for the NNS operation. */
                                candidate.produces.contains(node.predicate.column) -> {
                                    val index = IndexScanPhysicalOperatorNode(node.groupId, candidate, node.predicate)
                                    var p: OperatorNode.Physical = DistancePhysicalOperatorNode(index, node.predicate)
                                    val delta = scan.columns.filter { !candidate.produces.contains(it) }
                                    if (delta.isNotEmpty()) {
                                        p = FetchPhysicalOperatorNode(p, candidate.parent, delta.toTypedArray())
                                    }
                                    return node.output?.copyWithOutput(p) ?: p
                                }

                                /* Case 3: Index only produces TupleIds. Column for NSS needs to be fetched in an extra step. */
                                else -> {
                                    val index = IndexScanPhysicalOperatorNode(node.groupId, candidate, node.predicate)
                                    val distance = DistancePhysicalOperatorNode(index, node.predicate)
                                    val p = FetchPhysicalOperatorNode(distance, scan.entity, scan.columns.filter { !candidate.produces.contains(it) }.toTypedArray())
                                    return node.output?.copyWithOutput(p) ?: p
                                }
                            }
                        }
                    }
                }
            }
        }
        return null
    }
}