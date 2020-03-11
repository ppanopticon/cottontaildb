package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.types.VectorValue

/**
 * Squared L2 or Euclidian distance between to vectors.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
object SquaredEuclidianDistance : DistanceKernel {
    override val cost: Double
        get() = 1.0

    /**
     * Calculates the squared L2 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): Double = a.distanceL2(b).pow(2).asDouble().value

    /**
     * Calculates the weighted, squared L2 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights A list of weights
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): Double = ((b-a).pow(2)* weights).sum().value
}