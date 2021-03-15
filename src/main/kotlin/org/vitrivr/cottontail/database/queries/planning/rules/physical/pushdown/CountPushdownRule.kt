package org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.CountProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityCountPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * Pushes the simple counting of entries in an [Entity] down.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
object CountPushdownRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is CountProjectionPhysicalOperatorNode

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is CountProjectionPhysicalOperatorNode) {
            val input = node.input
            if (input is EntityScanPhysicalOperatorNode) {
                val p = EntityCountPhysicalOperatorNode(input.groupId, input.entity)
                return node.output?.copyWithOutput(p) ?: p
            }
        }
        return null
    }
}