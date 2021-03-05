package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sources.IndexScanOperator
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [NullaryPhysicalOperatorNode] that formalizes a scan of a physical [Index] in Cottontail DB on a given range.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class RangedIndexScanPhysicalOperatorNode(override val groupId: Int, val index: Index, val predicate: Predicate, val range: LongRange) : NullaryPhysicalOperatorNode() {
    companion object {
        private const val NODE_NAME = "ScanIndex"
    }

    /** The name of this [RangedIndexScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME


    override val outputSize = (this.range.last - this.range.first)
    override val statistics: RecordStatistics = this.index.parent.statistics
    override val columns: Array<ColumnDef<*>> = this.index.produces
    override val executable: Boolean = true
    override val canBePartitioned: Boolean = false
    override val cost = this.index.cost(this.predicate)

    init {
        require(this.range.first >= 0L) { "Start of a ranged index scan must be greater than zero." }
    }

    /**
     * Creates and returns a copy of this [RangedIndexScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [RangedIndexScanPhysicalOperatorNode].
     */
    override fun copy() = RangedIndexScanPhysicalOperatorNode(this.groupId, this.index, this.predicate, this.range)

    /**
     * Converts this [RangedIndexScanPhysicalOperatorNode] to a [IndexScanOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = IndexScanOperator(this.groupId, this.index, this.predicate, this.range)

    /**
     * [RangedIndexScanPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<Physical> {
        throw UnsupportedOperationException("RangedIndexScanPhysicalOperatorNode cannot be further partitioned.")
    }

    /**
     * Binds values from the provided [BindingContext] to this [RangedIndexScanPhysicalOperatorNode]'s [Predicate].
     *
     * @param ctx The [BindingContext] used for value binding.
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx)
    }

    /** Generates and returns a [String] representation of this [RangedIndexScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.index.type},${this.predicate},${this.range}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RangedIndexScanPhysicalOperatorNode) return false

        if (index != other.index) return false
        if (predicate != other.predicate) return false
        if (range != other.range) return false

        return true
    }

    override fun hashCode(): Int {
        var result = index.hashCode()
        result = 31 * result + predicate.hashCode()
        result = 31 * result + range.hashCode()
        return result
    }
}