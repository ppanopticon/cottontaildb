package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * Calculates the cosine distance, i.e. the angle, between two vectors.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
object CosineDistance : DistanceKernel {

    override val cost = 5.00f

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