package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

object AbsoluteInnerProductDistance : DistanceKernel {
    /**
     * Estimates of the cost incurred by applying this [CosineDistance] to a [VectorValue] of size [d].
     *
     * @param d The dimension to calculate the cost for.
     * @return The estimated cost.
     */
    override fun costForDimension(d: Int): Float = 2.25f * Cost.COST_FLOP + (Cost.COST_FLOP / d)

    /**
     * Calculates the asolute value of the inner product between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @return Absolute value of inner product between a and b.
     */
    override fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue = DoubleValue.ONE - (a dot b).abs().asDouble()

    /**
     * Calculates the absolute value of the weighted inner product between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights The per component weight (usually between 0.0 and 1.0)
     * @return Absolute value of weighted inner product between a and b.
     */
    override fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue {
        TODO("Not yet implemented")
    }
}