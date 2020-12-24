package org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates

import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.predicates.FilterOperator
import org.vitrivr.cottontail.execution.operators.predicates.ParallelFilterOperator

/**
 * A [UnaryPhysicalNodeExpression] that represents application of a [BooleanPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class FilterPhysicalNodeExpression(val predicate: BooleanPredicate, val selectivity: Float = Cost.COST_DEFAULT_SELECTIVITY) : UnaryPhysicalNodeExpression() {

    companion object {
        const val MAX_PARALLELISATION = 4
    }


    override val outputSize: Long
        get() = (this.input.outputSize * this.selectivity).toLong()

    override val cost: Cost
        get() = Cost(cpu = this.input.outputSize * this.predicate.cost * Cost.COST_MEMORY_ACCESS)

    override fun copy() = FilterPhysicalNodeExpression(this.predicate, this.selectivity)
    override fun toOperator(engine: TransactionManager): Operator {
        val parallelisation = Integer.min(this.cost.parallelisation(), MAX_PARALLELISATION)
        return if (parallelisation > 1) {
            val operators = this.input.partition(parallelisation).map { it.toOperator(engine) }
            ParallelFilterOperator(operators, this.predicate)
        } else {
            FilterOperator(this.input.toOperator(engine), this.predicate)
        }
    }
}