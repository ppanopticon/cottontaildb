package org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.BinaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterOnSubSelectLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.statistics.selectivity.NaiveSelectivityCalculator
import org.vitrivr.cottontail.database.statistics.selectivity.Selectivity
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.predicates.FilterOnSubselectOperator
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [BinaryLogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * As opposed to [FilterPhysicalOperatorNode], the [FilterOnSubSelectPhysicalOperatorNode] depends on
 * the execution of one or many sub-queries.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class FilterOnSubSelectPhysicalOperatorNode(val predicate: BooleanPredicate, vararg inputs: OperatorNode.Physical) : NAryPhysicalOperatorNode(*inputs) {
    companion object {
        private const val NODE_NAME = "Filter"
    }

    /** The name of this [FilterOnSubSelectPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [inputArity] of a [FilterOnSubSelectPhysicalOperatorNode] depends on the [BooleanPredicate]. */
    override val inputArity: Int = this.predicate.atomics.count { it is BooleanPredicate.Atomic.Literal && it.dependsOn != -1 } + 1

    /** The [FilterOnSubSelectPhysicalOperatorNode] returns the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>>
        get() = this.inputs[0].columns

    /** The [FilterOnSubSelectPhysicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: Array<ColumnDef<*>> = this.predicate.columns.toTypedArray()

    /** The [FilterOnSubSelectPhysicalOperatorNode] can only be executed if it doesn't contain any [ComparisonOperator.Binary.Match]. */
    override val executable: Boolean
        get() = super.executable && this.predicate.atomics.none { it.operator is ComparisonOperator.Binary.Match }

    /** The estimated output size of this [FilterOnSubSelectPhysicalOperatorNode]. Calculated based on [Selectivity] estimates. */
    override val outputSize: Long
        get() = NaiveSelectivityCalculator.estimate(this.predicate, this.statistics).invoke(this.inputs[0].outputSize)

    /** The [Cost] of this [FilterOnSubSelectPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(cpu = this.predicate.atomicCpuCost) * (this.inputs.firstOrNull()?.outputSize ?: 0)

    /**
     * Creates and returns a copy of this [FilterOnSubSelectPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [FilterOnSubSelectPhysicalOperatorNode].
     */
    override fun copy() = FilterOnSubSelectPhysicalOperatorNode(predicate = this.predicate)

    /**
     * [FilterOnSubSelectPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<Physical> {
        throw UnsupportedOperationException("FilterOnSubSelectPhysicalOperatorNode's cannot be partitioned.")
    }

    /**
     * Converts this [FilterPhysicalOperatorNode] to a [FilterOnSubselectOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = FilterOnSubselectOperator(
        this.inputs[0].toOperator(tx, ctx),
        this.inputs.drop(1).map { it.toOperator(tx, ctx) },
        this.predicate.bindValues(ctx.values)
    )

    /**
     * Binds values from the provided [BindingContext] to this [FilterPhysicalOperatorNode]'s [BooleanPredicate].
     *
     * @param ctx The [BindingContext] used for value binding.
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx) /* Important! */
    }

    /** Generates and returns a [String] representation of this [FilterOnSubSelectLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.predicate}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FilterOnSubSelectPhysicalOperatorNode) return false
        if (predicate != other.predicate) return false
        return true
    }

    override fun hashCode(): Int = this.predicate.hashCode()
}