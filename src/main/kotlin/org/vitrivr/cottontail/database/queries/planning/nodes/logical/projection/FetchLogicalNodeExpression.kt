package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [UnaryLogicalNodeExpression] that represents fetching certain [ColumnDef] from a specific
 * [Entity] and adding them to the list of requested columns.
 *
 * This can be used for late population, which can lead to optimized performance for kNN queries
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FetchLogicalNodeExpression(val entity: Entity, val fetch: Array<ColumnDef<*>>) : UnaryLogicalNodeExpression() {
    override fun copy() = FetchLogicalNodeExpression(this.entity, this.fetch)
}