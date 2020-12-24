package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A logical node expression that represents an arbitrary source of [Record]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RecordSourceLogicalNodeExpression(val columns: Array<ColumnDef<*>>) : UnaryLogicalNodeExpression() {
    override fun copy(): LogicalNodeExpression = RecordSourceLogicalNodeExpression(this.columns)
}