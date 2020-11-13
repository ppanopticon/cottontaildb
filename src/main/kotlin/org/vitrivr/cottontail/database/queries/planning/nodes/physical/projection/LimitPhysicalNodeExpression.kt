package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.transform.LimitOperator
import kotlin.math.min

/**
 * A [UnaryPhysicalNodeExpression] that represents the application of a LIMIT or SKIP clause on the result.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class LimitPhysicalNodeExpression(limit: Long, skip: Long) : UnaryPhysicalNodeExpression() {

    val limit = if (limit.coerceAtLeast(0) == 0L) {
        Long.MAX_VALUE
    } else {
        limit
    }

    val skip = if (limit.coerceAtLeast(0) == 0L) {
        0L
    } else {
        skip
    }

    override val outputSize: Long
        get() = min((this.input.outputSize - this.skip), this.limit)

    override val cost: Cost
        get() = Cost(cpu = this.outputSize * Cost.COST_MEMORY_ACCESS)

    override fun copy() = LimitPhysicalNodeExpression(this.limit, this.skip)

    override fun toOperator(context: ExecutionEngine.ExecutionContext) = LimitOperator(this.input.toOperator(context), context, this.skip, this.limit)
}