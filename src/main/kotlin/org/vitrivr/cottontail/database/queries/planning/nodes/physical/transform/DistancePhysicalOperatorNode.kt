package org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicateHint
import org.vitrivr.cottontail.database.statistics.columns.DoubleValueStatistics
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.projection.DistanceProjectionOperator
import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a [KnnPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class DistancePhysicalOperatorNode(input: Physical? = null, val predicate: KnnPredicate) : UnaryPhysicalOperatorNode(input) {
    companion object {
        private const val NODE_NAME = "Distance"
    }

    /** The name of this [DistancePhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [DistancePhysicalOperatorNode] returns the [ColumnDef] of its input + a distance column. */
    override val columns: Array<ColumnDef<*>>
        get() = super.columns + KnnUtilities.distanceColumnDef(this.predicate.column.name.entity())

    /** The [DistancePhysicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: Array<ColumnDef<*>> = this.predicate.columns.toTypedArray()

    /** The [Cost] of a [DistancePhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(cpu = this.outputSize * this.predicate.atomicCpuCost)

    /** The [RecordStatistics] for this [DistanceProjectionOperator]. Contains an empty [DoubleValueStatistics] for the distance column. */
    override val statistics: RecordStatistics
        get() {
            val copy = this.input?.statistics?.copy() ?: RecordStatistics()
            copy[this.predicate.produces] = DoubleValueStatistics() as ValueStatistics<Value>
            return copy
        }

    /** Whether the [DistanceProjectionOperator] can be partitioned is determined by the [KnnPredicateHint]. */
    override val canBePartitioned: Boolean
        get() {
            if (this.predicate.hint is KnnPredicateHint.ParallelKnnHint) {
                if (this.predicate.hint.max <= 1) return false
            }
            return super.canBePartitioned
        }

    /**
     * Creates and returns a copy of this [DistancePhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [DistancePhysicalOperatorNode].
     */
    override fun copy() = DistancePhysicalOperatorNode(predicate = this.predicate)

    /**
     * Partitions this [DistancePhysicalOperatorNode]. The number of partitions can be override by a [KnnPredicateHint.ParallelKnnHint]
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> {
        val input = this.input ?: throw IllegalStateException("Cannot partition disconnected OperatorNode (node = $this)")
        return if (this.predicate.hint is KnnPredicateHint.ParallelKnnHint) {
            val actual = p.coerceAtLeast(this.predicate.hint.min).coerceAtMost(this.predicate.hint.max)
            input.partition(actual).map { DistancePhysicalOperatorNode(it, this.predicate) }
        } else {
            input.partition(p).map { DistancePhysicalOperatorNode(it, this.predicate) }
        }
    }

    /**
     * Converts this [DistancePhysicalOperatorNode] to a [DistanceProjectionOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator {
        val predicate = this.predicate.bindValues(ctx.values)
        val input = this.input?.toOperator(tx, ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return DistanceProjectionOperator(input, predicate.column, this.predicate.toKernel() as DistanceKernel<VectorValue<*>>)
    }

    /**
     * Binds values from the provided [BindingContext] to this [DistancePhysicalOperatorNode]'s [KnnPredicate].
     *
     * @param ctx The [BindingContext] used for value binding.
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx) /* Important! */
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DistancePhysicalOperatorNode) return false

        if (predicate != other.predicate) return false

        return true
    }

    override fun hashCode(): Int {
        return predicate.hashCode()
    }
}