package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.DeleteOperator

/**
 * A [DeletePhysicalNodeExpression] that formalizes a delete operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class DeletePhysicalNodeExpression(val entity: Entity) : UnaryPhysicalNodeExpression() {
    override val outputSize: Long = 1L

    override val cost: Cost
        get() = Cost(io = this.entity.allColumns().size * this.input.outputSize * Cost.COST_DISK_ACCESS_WRITE)

    override fun copy(): DeletePhysicalNodeExpression = DeletePhysicalNodeExpression(this.entity)

    override fun toOperator(context: ExecutionEngine.ExecutionContext): Operator = DeleteOperator(this.input.toOperator(context), context, this.entity)
}