package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.DeleteOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [DeletePhysicalNodeExpression] that formalizes a delete operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DeletePhysicalNodeExpression(val entity: Entity) : UnaryPhysicalNodeExpression() {
    /** The [DeletePhysicalNodeExpression] produces the [ColumnDef]s defined in the [DeleteOperator]. */
    override val columns: Array<ColumnDef<*>> = DeleteOperator.COLUMNS

    /** The [DeletePhysicalNodeExpression] produces a single record. */
    override val outputSize: Long = 1L

    override val cost: Cost
        get() = Cost(io = this.entity.statistics.columns * this.input.outputSize * Cost.COST_DISK_ACCESS_WRITE)

    override fun copy(): DeletePhysicalNodeExpression = DeletePhysicalNodeExpression(this.entity)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator =
        DeleteOperator(this.input.toOperator(tx, ctx), this.entity)

    /**
     * Calculates and returns the digest for this [DeletePhysicalNodeExpression].
     *
     * @return Digest for this [DeletePhysicalNodeExpression]e
     */
    override fun digest(): Long = 31L * super.digest() + this.entity.hashCode()
}