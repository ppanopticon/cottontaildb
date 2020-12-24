package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.operators.basics.Operator

/**
 * A physical [NodeExpression] in the Cottontail DB query execution plan.
 *
 * [PhysicalNodeExpression]s are a proxy to a corresponding [ExecutionStage]. They belong to the
 * second phase of the query optimization process, in which [LogicalNodeExpression]s are replaced
 * by [PhysicalNodeExpression]s so as to generate an executable plan.
 *
 * [PhysicalNodeExpression] are associated with a cost model that allows the query planner to select
 * the optimal plan.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 *
 * @see NodeExpression
 * @see PhysicalNodeExpression
 */

abstract class PhysicalNodeExpression : NodeExpression() {
    /** [PhysicalNodeExpression]s are executable if all their inputs are executable. */
    override val executable: Boolean
        get() = this.inputs.all { it.executable }

    /** The estimated number of rows this [PhysicalNodeExpression] generates. */
    abstract val outputSize: Long

    /** An estimation of the [Cost] incurred by this [PhysicalNodeExpression]. */
    abstract val cost: Cost

    /** An estimation of the [Cost] incurred by the tree up and until this [PhysicalNodeExpression]. */
    val totalCost: Cost
        get() = if (this.inputs.isEmpty()) {
            this.cost
        } else {
            var cost = this.cost
            for (p in this.inputs) {
                if (p is PhysicalNodeExpression) {
                    cost += p.totalCost
                }
            }
            cost
        }

    /** True, if this [PhysicalNodeExpression] can be partitioned, false otherwise. */
    abstract val canBePartitioned: Boolean

    /**
     * Creates and returns a copy of this [PhysicalNodeExpression] without any children or parents.
     *
     * @return Copy of this [PhysicalNodeExpression].
     */
    abstract override fun copy(): PhysicalNodeExpression

    /**
     * Tries to create [p] partitions of this [PhysicalNodeExpression] if possible. If the implementing
     * [PhysicalNodeExpression] returns true for [canBePartitioned], then this method is expected
     * to return a result, even if the number of partitions returned my be lower than [p] or even one (which
     * means that no partitioning took place).
     *
     * If [canBePartitioned] returns false, this method is expected to throw a [IllegalStateException].
     *
     * @param p The desired number of partitions.
     * @return Array of [PhysicalNodeExpression]s.
     *
     * @throws IllegalStateException If this [PhysicalNodeExpression] cannot be partitioned.
     */
    abstract fun partition(p: Int): List<PhysicalNodeExpression>

    /**
     * Converts this [PhysicalNodeExpression] to the corresponding [Operator].
     *
     * @param engine The [TransactionManager] the [Operator] should be executed in.
     * @return [Operator]
     */
    abstract fun toOperator(engine: TransactionManager): Operator
}