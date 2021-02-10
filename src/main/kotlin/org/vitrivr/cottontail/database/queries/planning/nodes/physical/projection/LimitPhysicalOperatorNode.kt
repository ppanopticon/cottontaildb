package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.transform.LimitOperator
import kotlin.math.min

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a LIMIT or SKIP clause on the result.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class LimitPhysicalOperatorNode(limit: Long, skip: Long) : UnaryPhysicalOperatorNode() {

    /** The [LimitPhysicalOperatorNode] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

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

    override fun copy() = LimitPhysicalOperatorNode(this.limit, this.skip)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext) =
        LimitOperator(this.input.toOperator(tx, ctx), this.skip, this.limit)

    /**
     * Calculates and returns the digest for this [LimitPhysicalOperatorNode].
     *
     * @return Digest for this [LimitPhysicalOperatorNode]e
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.limit.hashCode()
        result = 31L * result + this.skip.hashCode()
        return result
    }
}