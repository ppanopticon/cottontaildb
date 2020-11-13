package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * Calculates the cosine distance, i.e. the angle, between two vectors.
 *
 * @version 1.0.1
 * @author Ralph Gasser
 */
object CosineDistance : DistanceKernel {

    /**
     * Estimates of the cost incurred by applying this [CosineDistance] to a [VectorValue] of size [d].
     *
     * @param d The dimension to calculate the cost for.
     * @return The estimated cost.
     */
    override fun costForDimension(d: Int): Float = 6.0f * Cost.COST_FLOP + ((3.0f / d) * Cost.COST_FLOP)

    /**
     * Calculates the L2 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue = DoubleValue.ONE - (((a dot b) / (a.norm2() * b.norm2())).asDouble())

    /**
     * Calculates the weighted L1 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights A list of weights
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue {
        val wa = (a * weights)
        val wb = (b * weights)
        return DoubleValue.ONE - (((wa dot wb) / (wa.norm2() * wb.norm2())).asDouble())
    }
}