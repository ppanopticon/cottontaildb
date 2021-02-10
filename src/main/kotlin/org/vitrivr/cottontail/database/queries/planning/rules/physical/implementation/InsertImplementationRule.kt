package org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.InsertLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.InsertPhysicalNodeExpression

/**
 * A [RewriteRule] that transforms a [InsertLogicalNodeExpression] to a [InsertPhysicalNodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object InsertImplementationRule : RewriteRule {
    override fun canBeApplied(node: NodeExpression): Boolean = node is InsertLogicalNodeExpression
    override fun apply(node: NodeExpression): NodeExpression? {
        if (node is InsertLogicalNodeExpression) {
            val p = InsertPhysicalNodeExpression(node.entity, node.records)
            node.copyOutput()?.addInput(p)
            return p
        }
        return null
    }
}