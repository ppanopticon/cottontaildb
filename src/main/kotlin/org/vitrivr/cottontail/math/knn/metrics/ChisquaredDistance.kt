package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

object ChisquaredDistance : DistanceKernel {

    /**
     * Estimates of the cost incurred by applying this [ChisquaredDistance] to a [VectorValue] of size [d].
     *
     * @param d The dimension to calculate the cost for.
     * @return The estimated cost.
     */
    override fun costForDimension(d: Int): Float = 5.0f * Cost.COST_FLOP

    override fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue = (((a - b).pow(2)) / (b + a)).sum().asDouble()

    override fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue = ((((a - b).pow(2)) / (a + b)) * weights).sum().asDouble()
}