package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * L2 or Euclidian distance between to vectors.
 *
 * @version 1.0.1
 * @author Ralph Gasser
 */
object EuclidianDistance : MinkowskiDistance {
    override val p: Int = 2

    /**
     * Estimates of the cost incurred by applying this [EuclidianDistance] to a [VectorValue] of size [d].
     *
     * @param d The dimension to calculate the cost for.
     * @return The estimated cost.
     */
    override fun costForDimension(d: Int): Float = 4.0f * Cost.COST_FLOP + (Cost.COST_FLOP / d)

    /**
     * Calculates the L2 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue = (a l2 b).asDouble()

    /**
     * Calculates the weighted L2 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights A list of weights
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue = ((a - b).pow(2) * (weights)).sum().sqrt().asDouble()
}