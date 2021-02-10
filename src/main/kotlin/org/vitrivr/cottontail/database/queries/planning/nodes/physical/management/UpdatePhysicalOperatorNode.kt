package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.ValueBinding
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.PhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.UpdateOperator

/**
 * A [UpdatePhysicalOperatorNode] that formalizes a UPDATE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class UpdatePhysicalOperatorNode(
    val entity: Entity,
    val values: List<Pair<ColumnDef<*>, ValueBinding>>
) : UnaryPhysicalOperatorNode() {

    /** The [UpdatePhysicalOperatorNode] produces the [ColumnDef]s defined in the [UpdateOperator]. */
    override val columns: Array<ColumnDef<*>> = UpdateOperator.COLUMNS

    /** The [UpdatePhysicalOperatorNode] produces a single record. */
    override val outputSize: Long = 1L

    override val cost: Cost
        get() = Cost(io = this.values.size * this.input.outputSize * Cost.COST_DISK_ACCESS_WRITE)

    override fun copy(): PhysicalOperatorNode = UpdatePhysicalOperatorNode(this.entity, this.values)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator {
        val entries = this.values.map { it.first to it.second.bind(ctx) } /* Late binding. */
        return UpdateOperator(this.input.toOperator(tx, ctx), this.entity, entries)
    }

    /**
     * Calculates and returns the digest for this [UpdatePhysicalOperatorNode].
     *
     * @return Digest for this [UpdatePhysicalOperatorNode]e
     */
    override fun digest(): Long {
        var result = 31L * super.digest()
        result = 31 * result + this.entity.hashCode()
        result = 31 * result + this.values.hashCode()
        return result
    }
}