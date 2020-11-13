package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.transform.FetchOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [UnaryPhysicalNodeExpression] that represents fetching certain [ColumnDef] from a specific
 * [Entity] and adding them to the list of requested columns.
 *
 * This can be used for late population, which can lead to optimized performance for kNN queries
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FetchPhysicalNodeExpression(val entity: Entity, val fetch: Array<ColumnDef<*>>) : UnaryPhysicalNodeExpression() {

    override val outputSize: Long
        get() = this.input.outputSize

    override val cost: Cost
        get() = Cost(this.outputSize * this.fetch.size * Cost.COST_DISK_ACCESS_READ, this.outputSize * Cost.COST_MEMORY_ACCESS, 0.0f)

    override fun copy() = FetchPhysicalNodeExpression(this.entity, this.fetch)

    override fun toOperator(context: ExecutionEngine.ExecutionContext) = FetchOperator(this.input.toOperator(context), context, this.entity, this.fetch)
}