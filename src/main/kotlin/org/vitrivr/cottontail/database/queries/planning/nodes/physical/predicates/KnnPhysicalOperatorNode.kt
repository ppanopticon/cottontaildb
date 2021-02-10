package org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicateHint
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.predicates.KnnOperator
import org.vitrivr.cottontail.execution.operators.predicates.ParallelKnnOperator
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a [KnnPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class KnnPhysicalOperatorNode(val predicate: KnnPredicate) : UnaryPhysicalOperatorNode() {

    /** The [KnnPhysicalOperatorNode] returns the [ColumnDef] of its input + a distance column. */
    override val columns: Array<ColumnDef<*>>
        get() = arrayOf(
            *this.input.columns,
            KnnUtilities.distanceColumnDef(this.predicate.column.name.entity())
        )


    /** The output size of a [KnnPhysicalOperatorNode] is k times the number of queries. */
    override val outputSize: Long
        get() = (this.predicate.k * this.predicate.query.size).toLong()

    /** The [Cost] of a [KnnPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(
            cpu = this.input.outputSize * this.predicate.distance.costForDimension(this.predicate.query.first().type.logicalSize) * (this.predicate.query.size + this.predicate.weights.size),
            memory = (this.outputSize * this.columns.map { it.type.physicalSize }.sum()).toFloat()
        )

    override fun copy() = KnnPhysicalOperatorNode(this.predicate)

    override fun bindValues(ctx: QueryContext): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx)
    }

    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator {
        val hint = this.predicate.hint
        val pMax = tx.availableThreads / 4
        val p = if (hint is KnnPredicateHint.ParallelKnnHint) {
            max(hint.min, min(pMax, hint.max))
        } else {
            min(this.cost.parallelisation(), pMax)
        }

        return if (p > 1 && this.input.canBePartitioned) {
            val partitions = this.input.partition(p)
            val operators = partitions.map {
                it.toOperator(tx, ctx)
            }
            ParallelKnnOperator(operators, this.predicate.bindValues(ctx))
        } else {
            KnnOperator(this.input.toOperator(tx, ctx), this.predicate.bindValues(ctx))
        }
    }

    /**
     * Calculates and returns the digest for this [KnnPhysicalOperatorNode].
     *
     * @return Digest for this [KnnPhysicalOperatorNode]e
     */
    override fun digest(): Long = 31L * super.digest() + this.predicate.digest()
}

