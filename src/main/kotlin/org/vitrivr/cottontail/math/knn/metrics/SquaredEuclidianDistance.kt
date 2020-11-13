package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * Squared L2 or Euclidian distance between to vectors.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
object SquaredEuclidianDistance : DistanceKernel {

    /**
     * Estimates of the cost incurred by applying this [SquaredEuclidianDistance] to a [VectorValue] of size [d].
     *
     * @param d The dimension to calculate the cost for.
     * @return The estimated cost.
     */
    override fun costForDimension(d: Int): Float = 4.0f * Cost.COST_FLOP

    /**
     * Calculates the squared L2 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue = (a l2 b).pow(2).asDouble()

    /**
     * Calculates the weighted, squared L2 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights A list of weights
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue = ((a - b).pow(2) * weights).sum().asDouble()
}