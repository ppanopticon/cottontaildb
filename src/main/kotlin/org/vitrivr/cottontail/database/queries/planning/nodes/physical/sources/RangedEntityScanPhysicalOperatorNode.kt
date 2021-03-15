package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntityScanOperator

/**
 * A [NullaryPhysicalOperatorNode] that formalizes a scan of a physical [Entity] in Cottontail DB on a given range.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class RangedEntityScanPhysicalOperatorNode(override val groupId: Int, val entity: Entity, override val columns: Array<ColumnDef<*>>, val range: LongRange) : NullaryPhysicalOperatorNode() {


    companion object {
        private const val NODE_NAME = "ScanEntity"
    }

    /** The name of this [RangedEntityScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME


    override val outputSize = (this.range.last - this.range.first)
    override val statistics: RecordStatistics = this.entity.statistics
    override val executable: Boolean = true
    override val canBePartitioned: Boolean = false
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS) * this.outputSize * this.columns.map {
        this.statistics[it].avgWidth
    }.sum()

    init {
        require(this.range.first >= 0L) { "Start of a ranged entity scan must be greater than zero." }
    }

    /**
     * Creates and returns a copy of this [RangedEntityScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [RangedEntityScanPhysicalOperatorNode].
     */
    override fun copy() = RangedEntityScanPhysicalOperatorNode(this.groupId, this.entity, this.columns, this.range)

    /**
     * [RangedIndexScanPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<OperatorNode.Physical> {
        throw UnsupportedOperationException("RangedEntityScanPhysicalOperatorNode cannot be further partitioned.")
    }

    /**
     * Converts this [RangedEntityScanPhysicalOperatorNode] to a [EntityScanOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = EntityScanOperator(this.groupId, this.entity, this.columns, this.range)

    /** Generates and returns a [String] representation of this [RangedEntityScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }},${this.range}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RangedEntityScanPhysicalOperatorNode) return false

        if (entity != other.entity) return false
        if (!columns.contentEquals(other.columns)) return false
        if (range != other.range) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + columns.contentHashCode()
        result = 31 * result + range.hashCode()
        return result
    }
}