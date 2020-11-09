package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.sources.EntityScanOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import kotlin.math.min

/**
 * A [UnaryPhysicalNodeExpression] that formalizes a scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
class EntityScanPhysicalNodeExpression(val entity: Entity, val columns: Array<ColumnDef<*>> = entity.allColumns().toTypedArray(), val range: LongRange? = null) : NullaryPhysicalNodeExpression() {
    /** Determines the output size of this [EntityScanPhysicalNodeExpression]. */
    override val outputSize = if (this.range != null) {
        require(this.range.first > 0L) { "Start of a ranged entity scan must be greater than zero." }
        this.range.last - this.range.first + 1L
    } else {
        this.entity.statistics().size
    }

    override val canBePartitioned: Boolean = true
    override val cost = Cost(this.outputSize * this.columns.size * Cost.COST_DISK_ACCESS_READ, this.outputSize * Cost.COST_MEMORY_ACCESS_READ)
    override fun copy() = EntityScanPhysicalNodeExpression(this.entity, this.columns, this.range)
    override fun toOperator(context: ExecutionEngine.ExecutionContext) = EntityScanOperator(context, this.entity, this.columns, this.range)
    override fun partition(p: Int): List<NullaryPhysicalNodeExpression> {
        val partitionSize = Math.floorDiv(this.outputSize, p.toLong())
        val start = this.range?.first ?: 0L
        val end = this.range?.last ?: this.entity.statistics().maxTupleId
        return (0 until p).map {
            EntityScanPhysicalNodeExpression(this.entity, this.columns, (start + it * partitionSize) until start + (min((it + 1L) * partitionSize, end)))
        }
    }
}