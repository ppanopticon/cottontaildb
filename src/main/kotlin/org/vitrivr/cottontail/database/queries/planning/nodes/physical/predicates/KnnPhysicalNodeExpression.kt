package org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates

import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.predicates.KnnPredicateHint
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.predicates.KnnOperator
import org.vitrivr.cottontail.execution.operators.predicates.ParallelKnnOperator
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [UnaryPhysicalNodeExpression] that represents the application of a [KnnPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class KnnPhysicalNodeExpression(val knn: KnnPredicate<*>) : UnaryPhysicalNodeExpression() {
    override val outputSize: Long
        get() = (this.knn.k * this.knn.query.size).toLong()

    override val cost: Cost
        get() = Cost(cpu = this.input.outputSize * this.knn.cost, memory = (this.outputSize * this.knn.columns.map { it.physicalSize }.sum()).toFloat())

    override fun copy() = KnnPhysicalNodeExpression(this.knn)
    override fun toOperator(engine: TransactionManager): Operator {
        val hint = this.knn.hint
        val pMax = engine.availableThreads / 4
        val p = if (hint is KnnPredicateHint.ParallelKnnPredicateHint) {
            max(hint.min, min(pMax, hint.max))
        } else {
            min(this.cost.parallelisation(), pMax)
        }
        return if (p > 1 && this.input.canBePartitioned) {
            val partitions = this.input.partition(p)
            val operators = partitions.map {
                it.toOperator(engine)
            }
            ParallelKnnOperator(operators, this.knn)
        } else {
            KnnOperator(this.input.toOperator(engine), this.knn)
        }
    }
}

