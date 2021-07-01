package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.AbstractIndex
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicateHint
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.database.statistics.selectivity.NaiveSelectivityCalculator
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sort.MergeLimitingHeapSortOperator
import org.vitrivr.cottontail.execution.operators.sources.IndexScanOperator
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [IndexScanPhysicalOperatorNode] that represents a predicated lookup using an [AbstractIndex].
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class IndexScanPhysicalOperatorNode(override val groupId: Int, val index: IndexTx, val predicate: Predicate) : NullaryPhysicalOperatorNode() {
    companion object {
        private const val NODE_NAME = "ScanIndex"
    }

    /** The name of this [IndexScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [ColumnDef]s produced by this [IndexScanPhysicalOperatorNode] depends on the [ColumnDef]s produced by the [Index]. */
    override val columns: Array<ColumnDef<*>> = this.index.dbo.produces

    /** [IndexScanPhysicalOperatorNode] are always executable. */
    override val executable: Boolean = true

    /** Whether an [IndexScanPhysicalOperatorNode] can be partitioned depends on the [Index]. */
    override val canBePartitioned: Boolean = this.index.dbo.supportsPartitioning

    /** The [RecordStatistics] is taken from the underlying [Entity]. [RecordStatistics] are used by the query planning for [Cost] estimation. */
    override val statistics: RecordStatistics = this.index.dbo.parent.statistics

    /** Cost estimation for [IndexScanPhysicalOperatorNode]s is delegated to the [Index]. */
    override val cost: Cost = this.index.dbo.cost(this.predicate)

    /** */
    override val outputSize: Long = when (this.predicate) {
        is BooleanPredicate -> NaiveSelectivityCalculator.estimate(this.predicate, this.statistics)(this.index.dbo.parent.numberOfRows)
        is KnnPredicate -> this.predicate.k.toLong()
        else -> this.index.dbo.parent.numberOfRows
    }

    /**
     * Creates and returns a copy of this [IndexScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [IndexScanPhysicalOperatorNode].
     */
    override fun copy() = IndexScanPhysicalOperatorNode(this.groupId, this.index, this.predicate)

    /**
     * Partitions this [IndexScanPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        check(this.index.dbo.supportsPartitioning) { "Index ${index.dbo.name} does not support partitioning!" }
        return (0 until p).map {
            RangedIndexScanPhysicalOperatorNode(this.groupId, this.index, this.predicate, it, p)
        }
    }

    /**
     * Converts this [IndexScanPhysicalOperatorNode] to a [IndexScanOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = when (this.predicate) {
        is KnnPredicate -> {
            val hint = this.predicate.hint
            val p = if (hint is KnnPredicateHint.ParallelKnnHint) {
                Integer.max(hint.min, this.totalCost.parallelisation(hint.max))
            } else {
                this.totalCost.parallelisation()
            }

            if (p > 1 && this.canBePartitioned) {
                val partitions = this.partition(p)
                val operators = partitions.map { it.toOperator(tx, ctx) }
                MergeLimitingHeapSortOperator(operators, arrayOf(Pair(this.predicate.produces, SortOrder.ASCENDING)), this.predicate.k.toLong())
            } else {
                IndexScanOperator(this.groupId, this.index, this.predicate.bindValues(ctx.values))
            }
        }
        is BooleanPredicate -> IndexScanOperator(this.groupId, this.index, this.predicate.bindValues(ctx.values))
        else -> throw UnsupportedOperationException("Unknown type of predicate ${this.predicate} cannot be converted to operator.")
    }

    /**
     * Binds values from the provided [BindingContext] to this [IndexScanPhysicalOperatorNode]'s [Predicate].
     *
     * @param ctx The [BindingContext] used for value binding.
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx)
    }

    /** Generates and returns a [String] representation of this [EntityScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.index.type},${this.predicate}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is IndexScanPhysicalOperatorNode) return false

        if (depth != other.depth) return false
        if (predicate != other.predicate) return false

        return true
    }

    override fun hashCode(): Int {
        var result = depth.hashCode()
        result = 31 * result + predicate.hashCode()
        return result
    }
}