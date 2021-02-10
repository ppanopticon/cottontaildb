package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.InsertLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.InsertPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that transforms a [InsertLogicalOperatorNode] to a [InsertPhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object InsertImplementationRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean = node is InsertLogicalOperatorNode
    override fun apply(node: OperatorNode): OperatorNode? {
        if (node is InsertLogicalOperatorNode) {
            val p = InsertPhysicalOperatorNode(node.entity, node.records)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}