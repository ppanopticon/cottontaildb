package org.vitrivr.cottontail.database.statistics.selectivity

import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ConnectionOperator
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics

/**
 * This is a very naive calculator for [Selectivity] values.
 *
 * It simply delegates [Selectivity] calculation for [BooleanPredicate.Atomic]to the [RecordStatistics] object of
 * the column and then combines these [Selectivity] values as if they were uncorrelated.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object NaiveSelectivityCalculator {
    /**
     * Estimates the selectivity of a [BooleanPredicate] given the [RecordStatistics].
     *
     * @param predicate The [BooleanPredicate] to evaluate.
     * @param statistics The [RecordStatistics] to use in the calculation.
     */
    fun estimate(predicate: BooleanPredicate, statistics: RecordStatistics): Selectivity = when (predicate) {
        is BooleanPredicate.Atomic.Literal -> estimateAtomicReference(predicate, statistics)
        is BooleanPredicate.Atomic.Reference -> estimateAtomicReference(predicate, statistics)
        is BooleanPredicate.Compound -> estimateCompoundSelectivity(predicate, statistics)
    }

    /**
     * Estimates the selectivity of a [BooleanPredicate.Atomic.Reference] given the [RecordStatistics].
     *
     * @param predicate The [BooleanPredicate.Atomic.Reference] to evaluate.
     * @param statistics The [RecordStatistics] to use in the calculation.
     */
    private fun estimateAtomicReference(predicate: BooleanPredicate.Atomic, statistics: RecordStatistics): Selectivity = statistics[predicate.left].estimateSelectivity(predicate)

    /**
     * Estimates the selectivity for a [BooleanPredicate.Compound].
     *
     * This is a very naive approach, which assumes that the [BooleanPredicate]s that make up
     * a [BooleanPredicate.Compound] are independent (i.e. there is no correlation between them).
     *
     * @param predicate The [BooleanPredicate.Compound] to evaluate.
     * @param statistics The [RecordStatistics] to use for the evaluation.
     */
    private fun estimateCompoundSelectivity(predicate: BooleanPredicate.Compound, statistics: RecordStatistics): Selectivity {
        val pp1 = estimate(predicate.p1, statistics)
        val pp2 = estimate(predicate.p2, statistics)
        return when (predicate.connector) {
            ConnectionOperator.AND -> pp1 * pp2
            ConnectionOperator.OR -> pp1 + pp2 - pp1 * pp2
        }
    }
}