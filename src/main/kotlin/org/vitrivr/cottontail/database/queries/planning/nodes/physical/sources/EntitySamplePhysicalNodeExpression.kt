package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.operators.sources.EntitySampleOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import kotlin.math.min

/**
 * A [NullaryPhysicalNodeExpression] that formalizes the random sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
data class EntitySamplePhysicalNodeExpression(val entity: Entity, val columns: Array<ColumnDef<*>> = entity.allColumns().toTypedArray(), val size: Long, val seed: Long = System.currentTimeMillis()) : NullaryPhysicalNodeExpression() {
    init {
        require(size > 0) { "Sample size must be greater than zero for sampling an entity." }
    }

    override val outputSize = this.size
    override val canBePartitioned: Boolean = true
    override val cost = Cost(this.outputSize * this.columns.size * Cost.COST_DISK_ACCESS_READ, this.size * Cost.COST_MEMORY_ACCESS)
    override fun copy() = EntitySamplePhysicalNodeExpression(this.entity, this.columns, this.size, this.seed)
    override fun toOperator(engine: TransactionManager) = EntitySampleOperator(this.entity, this.columns, this.size, this.seed)
    override fun partition(p: Int): List<NullaryPhysicalNodeExpression> {
        val partitionSize: Long = Math.floorDiv(this.size, p.toLong()) + 1L
        return (0 until p).map {
            val start = it * partitionSize
            val end = min((it + 1L) * partitionSize, this.size)
            EntitySamplePhysicalNodeExpression(this.entity, this.columns, end - start + 1)
        }
    }
}