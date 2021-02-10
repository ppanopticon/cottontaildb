package org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.ProjectionLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityCountPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.projection.Projection

/**
 * Pushes the simple counting of entries in an [Entity] down.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object CountPushdownRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is ProjectionLogicalNodeExpression && node.type == Projection.COUNT
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is ProjectionLogicalNodeExpression && node.type == Projection.COUNT) {
            val input = node.input
            if (input is EntityScanLogicalNodeExpression) {
                val p = EntityCountPhysicalNodeExpression(input.entity)
                node.copyOutput()?.addInput(p)
                return p
            }
        }
        return null
    }
}