package org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates

import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.predicates.FilterOperator

/**
 * A [UnaryPhysicalNodeExpression] that represents application of a [BooleanPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FilterPhysicalNodeExpression(val predicate: BooleanPredicate, val selectivity: Float = Cost.COST_DEFAULT_SELECTIVITY) : UnaryPhysicalNodeExpression() {

    override val outputSize: Long
        get() = (this.input.outputSize * this.selectivity).toLong()

    override val cost: Cost
        get() = Cost(cpu = this.input.outputSize * this.predicate.cost)

    override fun copy() = FilterPhysicalNodeExpression(this.predicate, this.selectivity)
    override fun toOperator(context: ExecutionEngine.ExecutionContext) = FilterOperator(this.input.toOperator(context), context, this.predicate)
}