package org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates

import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.predicates.KnnOperator
import org.vitrivr.cottontail.execution.operators.predicates.ParallelKnnOperator
import java.lang.Integer.min
import kotlin.math.roundToInt

/**
 * A [UnaryPhysicalNodeExpression] that represents the application of a [KnnPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class KnnPhysicalNodeExpression(val knn: KnnPredicate<*>) : UnaryPhysicalNodeExpression() {
    override val outputSize: Long
        get() = (this.knn.k * this.knn.query.size).toLong()

    override val cost: Cost
        get() = Cost(cpu = this.input.outputSize * this.knn.cost, memory = (this.outputSize * this.knn.columns.map { it.physicalSize }.sum()).toFloat())

    override fun copy() = KnnPhysicalNodeExpression(this.knn)
    override fun toOperator(context: ExecutionEngine.ExecutionContext): Operator {
        if (this.cost.cpu > 1.0f) {
            val base = seekBase(this.input)
            return if (base is NullaryPhysicalNodeExpression && base.canBePartitioned) {
                val partitions = base.partition(min(this.cost.cpu.roundToInt(), context.availableThreads))
                val operators = partitions.map {
                    var prev: NodeExpression? = null
                    var next: NodeExpression = this.input
                    while (next != base) {
                        if (prev != null) {
                            next.addInput(prev)
                        }
                        prev = next.copy()
                        next = prev.inputs.first()
                    }
                    it.toOperator(context)
                }
                ParallelKnnOperator(operators, context, this.knn)
            } else {
                KnnOperator(this.input.toOperator(context), context, this.knn)
            }
        } else {
            return KnnOperator(this.input.toOperator(context), context, this.knn)
        }
    }
}

