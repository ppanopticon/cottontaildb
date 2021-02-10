package org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.ProjectionLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityCountPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.database.queries.projection.Projection

/**
 * Pushes the simple counting of entries in an [Entity] down.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object CountPushdownRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean =
        node is ProjectionLogicalOperatorNode && node.type == Projection.COUNT

    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is ProjectionLogicalOperatorNode && node.type == Projection.COUNT) {
            val input = node.input
            if (input is EntityScanLogicalOperatorNode) {
                val p = EntityCountPhysicalOperatorNode(input.entity)
                node.copyOutput()?.addInput(p)
                return p
            }
        }
        return null
    }
}