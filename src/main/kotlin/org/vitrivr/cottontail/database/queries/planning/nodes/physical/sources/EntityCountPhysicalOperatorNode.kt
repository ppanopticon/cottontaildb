package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntityCountOperator
import org.vitrivr.cottontail.model.basics.Type

/**
 * A [NullaryPhysicalOperatorNode] that formalizes the counting entries in a physical [Entity].
 *
 * @author Ralph Gasser
 * @version 2.1.1
 */
class EntityCountPhysicalOperatorNode(override val groupId: Int, val entity: EntityTx) : NullaryPhysicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "CountEntity"
    }

    /** The name of this [EntityCountPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The output size of an [EntityCountPhysicalOperatorNode] is always one. */
    override val outputSize = 1L

    /** */
    override val columns: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.entity.dbo.name.column(Projection.COUNT.label()), Type.Long, false))

    /** [EntityCountPhysicalOperatorNode] is always executable. */
    override val executable: Boolean = true

    /** [EntityCountPhysicalOperatorNode] cannot be partitioned. */
    override val canBePartitioned: Boolean = false

    /** The estimated [Cost] of sampling the [Entity]. */
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS)

    /** The [RecordStatistics] is taken from the underlying [Entity]. [RecordStatistics] are used by the query planning for [Cost] estimation. */
    override val statistics: RecordStatistics = this.entity.dbo.statistics

    /**
     * Creates and returns a copy of this [EntityCountPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntityCountPhysicalOperatorNode].
     */
    override fun copy() = EntityCountPhysicalOperatorNode(this.groupId, this.entity)

    /**
     * [EntityCountPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        throw UnsupportedOperationException("EntityCountPhysicalNodeExpression cannot be partitioned.")
    }

    /**
     * Converts this [EntityCountPhysicalOperatorNode] to a [EntityCountOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = EntityCountOperator(this.groupId, this.entity)

    /** Generates and returns a [String] representation of this [EntityCountPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntityCountPhysicalOperatorNode) return false
        if (entity != other.entity) return false
        return true
    }

    override fun hashCode(): Int = this.entity.hashCode()
}