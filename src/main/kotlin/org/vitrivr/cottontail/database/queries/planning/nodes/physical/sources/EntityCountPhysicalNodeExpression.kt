package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.sources.EntityCountOperator

/**
 * A [NullaryPhysicalNodeExpression] that formalizes the counting entries in a physical [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
data class EntityCountPhysicalNodeExpression(val entity: Entity) : NullaryPhysicalNodeExpression() {
    override val outputSize = 1L
    override val canBePartitioned: Boolean = false
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS)
    override fun copy() = EntityCountPhysicalNodeExpression(this.entity)
    override fun toOperator(context: ExecutionEngine.ExecutionContext) = EntityCountOperator(context, this.entity)
    override fun partition(p: Int): List<NullaryPhysicalNodeExpression> {
        throw IllegalStateException("EntityCountPhysicalNodeExpression cannot be partitioned.")
    }
}