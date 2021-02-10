package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.ValueBinding
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.PhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [UpdatePhysicalNodeExpression] that formalizes a UPDATE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class UpdatePhysicalNodeExpression(
    val entity: Entity,
    val values: List<Pair<ColumnDef<*>, ValueBinding>>
) : UnaryPhysicalNodeExpression() {

    /** The [UpdatePhysicalNodeExpression] produces the [ColumnDef]s defined in the [UpdateOperator]. */
    override val columns: Array<ColumnDef<*>> = UpdateOperator.COLUMNS

    /** The [UpdatePhysicalNodeExpression] produces a single record. */
    override val outputSize: Long = 1L

    override val cost: Cost
        get() = Cost(io = this.values.size * this.input.outputSize * Cost.COST_DISK_ACCESS_WRITE)

    override fun copy(): PhysicalNodeExpression =
        UpdatePhysicalNodeExpression(this.entity, this.values)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator {
        val entries = this.values.map { it.first to it.second.apply(ctx) } /* Late binding. */
        return UpdateOperator(this.input.toOperator(tx, ctx), this.entity, entries)
    }

    /**
     * Calculates and returns the digest for this [UpdatePhysicalNodeExpression].
     *
     * @return Digest for this [UpdatePhysicalNodeExpression]e
     */
    override fun digest(): Long {
        var result = 31L * super.digest()
        result = 31 * result + this.entity.hashCode()
        result = 31 * result + this.values.hashCode()
        return result
    }
}