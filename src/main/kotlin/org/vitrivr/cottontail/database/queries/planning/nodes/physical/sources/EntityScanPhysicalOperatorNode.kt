package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntityScanOperator

/**
 * A [UnaryPhysicalOperatorNode] that formalizes a scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.1.1
 */
class EntityScanPhysicalOperatorNode(override val groupId: Int, val entity: EntityTx, override val columns: Array<ColumnDef<*>>) : NullaryPhysicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "ScanEntity"
    }

    /** The name of this [EntityScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The number of rows returned by this [EntityScanPhysicalOperatorNode] equals to the number of rows in the [Entity]. */
    override val outputSize = this.entity.count()

    /** [EntityScanPhysicalOperatorNode] is always executable. */
    override val executable: Boolean = true

    /** [EntityScanPhysicalOperatorNode] can always be partitioned. */
    override val canBePartitioned: Boolean = true

    /** The [RecordStatistics] is taken from the underlying [Entity]. [RecordStatistics] are used by the query planning for [Cost] estimation. */
    override val statistics: RecordStatistics = this.entity.snapshot.statistics

    /** The estimated [Cost] of scanning the [Entity]. */
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS) * this.outputSize * this.columns.map {
        this.statistics[it].avgWidth
    }.sum()

    /**
     * Creates and returns a copy of this [EntityScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntityScanPhysicalOperatorNode].
     */
    override fun copy() = EntityScanPhysicalOperatorNode(this.groupId, this.entity, this.columns)

    /**
     * Partitions this [EntityScanPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        return (0 until p).map { RangedEntityScanPhysicalOperatorNode(this.groupId, this.entity, this.columns, it, p) }
    }

    /**
     * Converts this [EntityScanPhysicalOperatorNode] to a [EntityScanOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = EntityScanOperator(this.groupId, this.entity, this.columns, 0, 1)

    /** Generates and returns a [String] representation of this [EntityScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntityScanPhysicalOperatorNode) return false

        if (entity != other.entity) return false
        if (!columns.contentEquals(other.columns)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + columns.contentHashCode()
        return result
    }
}