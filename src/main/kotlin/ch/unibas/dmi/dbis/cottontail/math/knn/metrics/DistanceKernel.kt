package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.types.VectorValue

interface DistanceKernel {
    /** Estimate of the cost required per vector component. */
    val cost: Double

    /**
     * Calculates the distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): Double

    /**
     * Calculates the distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): Double
}