package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntityScanOperator

/**
 * A [UnaryPhysicalOperatorNode] that formalizes a scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class EntityScanPhysicalOperatorNode(
    val entity: Entity,
    override val columns: Array<ColumnDef<*>>
) : NullaryPhysicalOperatorNode() {
    override val outputSize = this.entity.statistics.rows
    override val canBePartitioned: Boolean = true
    override val cost = Cost(
        this.outputSize * this.columns.size * Cost.COST_DISK_ACCESS_READ,
        this.outputSize * this.columns.size * Cost.COST_MEMORY_ACCESS
    )

    override fun copy() = EntityScanPhysicalOperatorNode(this.entity, this.columns)
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) =
        EntityScanOperator(this.entity, this.columns)

    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        val partitionSize = Math.floorDiv(this.outputSize, p.toLong())
        return (0 until p).map {
            val start = (it * partitionSize)
            val end = ((it + 1) * partitionSize) - 1
            RangedEntityScanPhysicalOperatorNode(this.entity, this.columns, start until end)
        }
    }

    /**
     * Calculates and returns the digest for this [EntityScanPhysicalOperatorNode].
     *
     * @return Digest for this [EntityScanPhysicalOperatorNode]e
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.entity.hashCode()
        result = 31L * result + this.columns.contentHashCode()
        return result
    }
}