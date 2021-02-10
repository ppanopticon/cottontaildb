package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that replaces a [EntityScanLogicalOperatorNode] by a [EntityScanPhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object EntityScanImplementationRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is EntityScanLogicalOperatorNode
    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is EntityScanLogicalOperatorNode) {
            val children = node.copyOutput()
            val p = EntityScanPhysicalOperatorNode(node.entity, node.columns)
            children?.addInput(p)
            return p
        }
        return null
    }
}