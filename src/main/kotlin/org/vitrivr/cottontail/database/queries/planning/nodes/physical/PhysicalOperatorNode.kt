package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator

/**
 * A physical [OperatorNode] in the Cottontail DB query execution plan.
 *
 * [PhysicalOperatorNode]s are a direct proxy to a corresponding [Operator]. They belong to the
 * second phase of the query optimization process, in which [LogicalOperatorNode]s are replaced
 * by [PhysicalOperatorNode]s so as to generate an executable plan.
 *
 * As opposed to [LogicalOperatorNode]s, [PhysicalOperatorNode]s are associated concrete implementations
 * and a cost model that allows  the query planner to select the optimal plan.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 *
 * @see OperatorNode
 */
abstract class PhysicalOperatorNode : OperatorNode() {
    /** [PhysicalOperatorNode]s are executable if all their inputs are executable. */
    override val executable: Boolean
        get() = this.inputs.all { it.executable }

    /** The estimated number of rows this [PhysicalOperatorNode] generates. */
    abstract val outputSize: Long

    /** An estimation of the [Cost] incurred by this [PhysicalOperatorNode]. */
    abstract val cost: Cost

    /** An estimation of the [Cost] incurred by the tree up and until this [PhysicalOperatorNode]. */
    val totalCost: Cost
        get() = if (this.inputs.isEmpty()) {
            this.cost
        } else {
            var cost = this.cost
            for (p in this.inputs) {
                if (p is PhysicalOperatorNode) {
                    cost += p.totalCost
                }
            }
            cost
        }

    /** True, if this [PhysicalOperatorNode] can be partitioned, false otherwise. */
    abstract val canBePartitioned: Boolean

    /**
     * Creates and returns a copy of this [PhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [PhysicalOperatorNode].
     */
    abstract override fun copy(): PhysicalOperatorNode

    /**
     * Tries to create [p] partitions of this [PhysicalOperatorNode] if possible. If the implementing
     * [PhysicalOperatorNode] returns true for [canBePartitioned], then this method is expected
     * to return a result, even if the number of partitions returned my be lower than [p] or even one (which
     * means that no partitioning took place).
     *
     * If [canBePartitioned] returns false, this method is expected to throw a [IllegalStateException].
     *
     * @param p The desired number of partitions.
     * @return Array of [PhysicalOperatorNode]s.
     *
     * @throws IllegalStateException If this [PhysicalOperatorNode] cannot be partitioned.
     */
    abstract fun partition(p: Int): List<PhysicalOperatorNode>

    /**
     * Converts this [PhysicalOperatorNode] to the corresponding [Operator].
     *
     * @param tx The [TransactionContext] the [Operator] should be executed in.
     * @param ctx: The [QueryContext] used for conversion. Mainly for value binding.
     *
     * @return [Operator]
     */
    abstract fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator

    /**
     * Calculates and returns the digest for this [PhysicalOperatorNode].
     *
     * @return Digest for this [PhysicalOperatorNode]
     */
    override fun digest(): Long = this.javaClass.hashCode().toLong()
}