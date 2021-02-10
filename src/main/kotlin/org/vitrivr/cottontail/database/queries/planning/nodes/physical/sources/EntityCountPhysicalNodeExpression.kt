package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntityCountOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [NullaryPhysicalNodeExpression] that formalizes the counting entries in a physical [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class EntityCountPhysicalNodeExpression(val entity: Entity) : NullaryPhysicalNodeExpression() {
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entity.name.column(Projection.COUNT.label()), Type.Long, true)
    )

    override val outputSize = 1L
    override val canBePartitioned: Boolean = false
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS)
    override fun copy() = EntityCountPhysicalNodeExpression(this.entity)
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) =
        EntityCountOperator(this.entity)

    override fun partition(p: Int): List<NullaryPhysicalNodeExpression> {
        throw IllegalStateException("EntityCountPhysicalNodeExpression cannot be partitioned.")
    }

    /**
     * Calculates and returns the digest for this [EntityCountPhysicalNodeExpression].
     *
     * @return Digest for this [EntityCountPhysicalNodeExpression]e
     */
    override fun digest(): Long = 31L * super.digest() + this.entity.hashCode()
}