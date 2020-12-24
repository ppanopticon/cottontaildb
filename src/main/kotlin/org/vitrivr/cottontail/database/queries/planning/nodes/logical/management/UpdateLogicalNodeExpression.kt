package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [DeleteLogicalNodeExpression] that formalizes an UPDATE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class UpdateLogicalNodeExpression(val entity: Entity, val values: List<Pair<ColumnDef<*>, Value?>>) : UnaryLogicalNodeExpression() {

    /**
     * Returns a copy of this [UpdateLogicalNodeExpression]
     *
     * @return Copy of this [UpdateLogicalNodeExpression]
     */
    override fun copy(): UpdateLogicalNodeExpression = UpdateLogicalNodeExpression(this.entity, this.values)
}