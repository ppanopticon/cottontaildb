package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * Calculates the Hamming distance between to vectors.
 *
 * @version 1.0.1
 * @author Ralph Gasser
 */
object HammingDistance : DistanceKernel {

    /**
     * Estimates of the cost incurred by applying this [ManhattanDistance] to a [VectorValue] of size [d].
     *
     * @param d The dimension to calculate the cost for.
     * @return The estimated cost.
     */
    override fun costForDimension(d: Int): Float = Cost.COST_MEMORY_ACCESS + Cost.COST_FLOP

    /**
     * Calculates the Hamming distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue = (a hamming b).asDouble()

    /**
     * Calculates the weighted Hamming distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights A list of weights
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue {
        var sum = 0.0
        for (i in 0 until a.logicalSize) {
            if (a[i] == b[i]) {
                sum += weights[i].value.toDouble()
            }
        }
        return DoubleValue(sum)
    }
}