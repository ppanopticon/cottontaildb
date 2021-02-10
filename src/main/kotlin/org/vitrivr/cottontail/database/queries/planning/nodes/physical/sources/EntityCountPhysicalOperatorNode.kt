package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntityCountOperator
import org.vitrivr.cottontail.model.basics.Type

/**
 * A [NullaryPhysicalOperatorNode] that formalizes the counting entries in a physical [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class EntityCountPhysicalOperatorNode(val entity: Entity) : NullaryPhysicalOperatorNode() {
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entity.name.column(Projection.COUNT.label()), Type.Long, true)
    )

    override val outputSize = 1L
    override val canBePartitioned: Boolean = false
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS)
    override fun copy() = EntityCountPhysicalOperatorNode(this.entity)
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) =
        EntityCountOperator(this.entity)

    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        throw IllegalStateException("EntityCountPhysicalNodeExpression cannot be partitioned.")
    }

    /**
     * Calculates and returns the digest for this [EntityCountPhysicalOperatorNode].
     *
     * @return Digest for this [EntityCountPhysicalOperatorNode]e
     */
    override fun digest(): Long = 31L * super.digest() + this.entity.hashCode()
}