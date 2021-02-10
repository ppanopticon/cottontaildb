package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.DeleteOperator

/**
 * A [DeletePhysicalOperatorNode] that formalizes a delete operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DeletePhysicalOperatorNode(val entity: Entity) : UnaryPhysicalOperatorNode() {
    /** The [DeletePhysicalOperatorNode] produces the [ColumnDef]s defined in the [DeleteOperator]. */
    override val columns: Array<ColumnDef<*>> = DeleteOperator.COLUMNS

    /** The [DeletePhysicalOperatorNode] produces a single record. */
    override val outputSize: Long = 1L

    override val cost: Cost
        get() = Cost(io = this.entity.statistics.columns * this.input.outputSize * Cost.COST_DISK_ACCESS_WRITE)

    override fun copy(): DeletePhysicalOperatorNode = DeletePhysicalOperatorNode(this.entity)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator =
        DeleteOperator(this.input.toOperator(tx, ctx), this.entity)

    /**
     * Calculates and returns the digest for this [DeletePhysicalOperatorNode].
     *
     * @return Digest for this [DeletePhysicalOperatorNode]e
     */
    override fun digest(): Long = 31L * super.digest() + this.entity.hashCode()
}