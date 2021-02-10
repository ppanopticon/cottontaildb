package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.PhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntityScanOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [UnaryPhysicalNodeExpression] that formalizes a scan of a physical [Entity] in Cottontail DB on a given range.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class RangedEntityScanPhysicalNodeExpression(
    val entity: Entity,
    override val columns: Array<ColumnDef<*>>,
    val range: LongRange
) : NullaryPhysicalNodeExpression() {
    init {
        require(this.range.first >= 0L) { "Start of a ranged entity scan must be greater than zero." }
    }

    override val outputSize = this.entity.statistics.rows
    override val canBePartitioned: Boolean = true
    override val cost = Cost(
        this.outputSize * this.columns.size * Cost.COST_DISK_ACCESS_READ,
        this.outputSize * this.columns.size * Cost.COST_MEMORY_ACCESS
    )

    override fun copy() =
        RangedEntityScanPhysicalNodeExpression(this.entity, this.columns, this.range)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext) =
        EntityScanOperator(this.entity, this.columns, this.range)

    override fun partition(p: Int): List<PhysicalNodeExpression> {
        throw IllegalStateException("RangedEntityScanPhysicalNodeExpression cannot be partitioned.")
    }
}