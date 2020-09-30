package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

object AbsoluteInnerProductDistance : DistanceKernel {
    /** Estimate of the cost required per vector component. */
    override val cost = 3.0f
    // todo: assess actual cost

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