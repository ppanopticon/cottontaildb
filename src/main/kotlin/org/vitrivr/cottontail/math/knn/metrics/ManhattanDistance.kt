package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * L1 or Manhattan distance between to vectors.
 *
 * @version 1.0.1
 * @author Ralph Gasser
 */
object ManhattanDistance : MinkowskiDistance {
    override val p: Int = 1

    /**
     * Estimates of the cost incurred by applying this [ManhattanDistance] to a [VectorValue] of size [d].
     *
     * @param d The dimension to calculate the cost for.
     * @return The estimated cost.
     */
    override fun costForDimension(d: Int): Float = d * 2.25f * Cost.COST_FLOP

    /**
     * Calculates the L1 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue = (a l1 b).asDouble()

    /**
     * Calculates the weighted L1 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights A list of weights
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue = ((a - b).abs() * weights).sum().asDouble()
}