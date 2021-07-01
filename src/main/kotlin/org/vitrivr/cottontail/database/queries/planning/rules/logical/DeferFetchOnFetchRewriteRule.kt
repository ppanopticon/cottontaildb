package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.BinaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NAryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform.FetchLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that defers fetching of columns fetched in a [FetchLogicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DeferFetchOnFetchRewriteRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is FetchLogicalOperatorNode
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is FetchLogicalOperatorNode) {
            val originalColumns = node.fetch
            val originalGroupId = node.groupId
            var copy = node.input!!.copyWithInputs()

            /* Traverse tree and push down FetchLogicalOperatorNode. */
            var next: OperatorNode.Logical? = node.output
            while (next != null && next.groupId == originalGroupId) {
                /* Append FetchLogicalOperatorNode for columns required by next element. */
                val required = originalColumns.filter { it in next!!.requires }.toTypedArray()
                if (required.isEmpty()) {
                    next = next.output /* No requirement necessary. */
                } else if (required.size == originalColumns.size) {
                    break /* No deferral possible; abort. */
                } else {
                    copy = FetchLogicalOperatorNode(copy, node.entity, required)
                    /* Append next element. */
                    when (next) {
                        is UnaryLogicalOperatorNode -> {
                            val p = next.copy()
                            p.input = copy
                            copy = p
                        }
                        is BinaryLogicalOperatorNode -> {
                            val p = next.copy()
                            p.left = copy
                            p.right = next.right?.copyWithInputs()
                            copy = p
                        }
                        is NAryLogicalOperatorNode -> {
                            val p = next.copy()
                            p.addInput(copy)
                            for (it in next.inputs.drop(1)) {
                                p.addInput(it.copyWithInputs())
                            }
                            copy = p
                        }
                        else -> throw IllegalArgumentException("Encountered unsupported node during execution of DeferredFetchRewriteRule.")
                    }

                    /* Append FetchLogicalOperatorNode for columns not required by next element and return. */
                    if (required.isNotEmpty()) {
                        val defer = originalColumns.filter { it !in required }.toTypedArray()
                        if (defer.isNotEmpty() && next.output != null) {
                            copy = FetchLogicalOperatorNode(copy, node.entity, defer)
                        }
                        return next.output?.copyWithOutput(copy) ?: copy
                    }

                }
            }
        }
        return null
    }
}