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
 * @version 1.0
 */
class EntityScanPhysicalNodeExpression(val entity: Entity, val columns: Array<ColumnDef<*>> = entity.allColumns().toTypedArray(), val range: LongRange = 1L..entity.statistics.maxTupleId) : NullaryPhysicalNodeExpression() {
    init {
        require(this.range.first > 0L) { "Start of a ranged entity scan must be greater than zero." }
    }

    override val outputSize = this.range.last - this.range.first
    override val canBePartitioned: Boolean = true
    override val cost = Cost(this.outputSize * this.columns.size * Cost.COST_DISK_ACCESS_READ, this.outputSize * Cost.COST_MEMORY_ACCESS_READ)
    override fun copy() = EntityScanPhysicalNodeExpression(this.entity, this.columns, this.range)
    override fun toOperator(context: ExecutionEngine.ExecutionContext) = EntityScanOperator(context, this.entity, this.columns, this.range)
    override fun partition(p: Int): List<NullaryPhysicalNodeExpression> {
        val partitionSize = Math.floorDiv(this.range.last - this.range.first + 1L, p.toLong())
        return (0 until p).map {
            val start = range.first + it * partitionSize
            val end = range.first + min((it + 1L) * partitionSize, this.range.last)
            EntityScanPhysicalNodeExpression(this.entity, this.columns, start until end)
        }
    }
}