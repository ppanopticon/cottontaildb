package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression

/**
 * A [DeleteLogicalNodeExpression] that formalizes a DELETE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DeleteLogicalNodeExpression(val entity: Entity): UnaryLogicalNodeExpression() {

    /**
     * Returns a copy of this [DeleteLogicalNodeExpression]
     *
     * @return Copy of this [DeleteLogicalNodeExpression]
     */
    override fun copy(): DeleteLogicalNodeExpression = DeleteLogicalNodeExpression(this.entity)
}