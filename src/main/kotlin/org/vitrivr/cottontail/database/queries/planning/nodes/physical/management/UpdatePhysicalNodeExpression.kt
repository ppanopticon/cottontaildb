package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.PhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [UpdatePhysicalNodeExpression] that formalizes a update operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class UpdatePhysicalNodeExpression(val entity: Entity, val values: List<Pair<ColumnDef<*>, Value?>>) : UnaryPhysicalNodeExpression() {
    override val outputSize: Long = 1L

    override val cost: Cost
        get() = Cost(io = this.values.size * this.input.outputSize * Cost.COST_DISK_ACCESS_WRITE)

    override fun copy(): PhysicalNodeExpression = UpdatePhysicalNodeExpression(this.entity, this.values)

    override fun toOperator(context: ExecutionEngine.ExecutionContext): Operator = UpdateOperator(this.input.toOperator(context), context, this.entity, this.values)
}