package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.DeleteLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.DeleteOperator

/**
 * A [DeletePhysicalOperatorNode] that formalizes a delete operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class DeletePhysicalOperatorNode(input: Physical? = null, val entity: EntityTx) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Delete"
    }

    /** The name of this [DeleteLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [DeletePhysicalOperatorNode] produces the [ColumnDef]s defined in the [DeleteOperator]. */
    override val columns: Array<ColumnDef<*>> = DeleteOperator.COLUMNS

    /** The [DeletePhysicalOperatorNode] produces a single record. */
    override val outputSize: Long = 1L

    /** The [Cost] of this [DeletePhysicalOperatorNode]. */
    override val cost: Cost = Cost(io = this.entity.count() * Cost.COST_DISK_ACCESS_WRITE) * (this.input?.outputSize ?: 0)

    /** The [DeletePhysicalOperatorNode]s cannot be partitioned. */
    override val canBePartitioned: Boolean = false

    /**
     * Creates and returns a copy of this [DeletePhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [DeletePhysicalOperatorNode].
     */
    override fun copy() = DeletePhysicalOperatorNode(entity = this.entity)

    /**
     * Converts this [DeletePhysicalOperatorNode] to a [DeleteOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = DeleteOperator(
        this.input?.toOperator(tx, ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)"),
        this.entity
    )

    /**
     * [DeletePhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<Physical> {
        throw UnsupportedOperationException("DeletePhysicalOperatorNode cannot be partitioned.")
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DeletePhysicalOperatorNode) return false

        if (entity != other.entity) return false

        return true
    }

    override fun hashCode(): Int = this.entity.hashCode()
}