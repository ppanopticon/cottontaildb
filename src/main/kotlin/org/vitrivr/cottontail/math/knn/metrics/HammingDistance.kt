package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * Calculates the Hamming distance between to vectors.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
object HammingDistance : DistanceKernel {
    override val cost = 1.0f


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