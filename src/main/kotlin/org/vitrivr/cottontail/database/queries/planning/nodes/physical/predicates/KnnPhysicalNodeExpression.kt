package org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates

import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.KnnPredicateBinding
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicateHint
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.predicates.KnnOperator
import org.vitrivr.cottontail.execution.operators.predicates.ParallelKnnOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [UnaryPhysicalNodeExpression] that represents the application of a [KnnPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class KnnPhysicalNodeExpression(val knn: KnnPredicateBinding) : UnaryPhysicalNodeExpression() {

    /** The [KnnPhysicalNodeExpression] returns the [ColumnDef] of its input + a distance column. */
    override val columns: Array<ColumnDef<*>>
        get() = arrayOf(
            *this.input.columns,
            KnnUtilities.distanceColumnDef(this.knn.column.name.entity())
        )


    /** The output size of a [KnnPhysicalNodeExpression] is k times the number of queries. */
    override val outputSize: Long
        get() = (this.knn.k * this.knn.query.size).toLong()

    /** The [Cost] of a [KnnPhysicalNodeExpression]. */
    override val cost: Cost
        get() = Cost(
            cpu = this.input.outputSize * this.knn.distance.costForDimension(this.knn.query.first().type.logicalSize) * (this.knn.query.size + (this.knn.weights?.size
                ?: 0)),
            memory = (this.outputSize * this.columns.map { it.type.physicalSize }.sum()).toFloat()
        )

    override fun copy() = KnnPhysicalNodeExpression(this.knn)
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator {
        val hint = this.knn.hint
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
            ParallelKnnOperator(operators, this.knn.apply(ctx))
        } else {
            KnnOperator(this.input.toOperator(tx, ctx), this.knn.apply(ctx))
        }
    }

    /**
     * Calculates and returns the digest for this [KnnPhysicalNodeExpression].
     *
     * @return Digest for this [KnnPhysicalNodeExpression]e
     */
    override fun digest(): Long = 31L * super.digest() + this.knn.hashCode()
}

