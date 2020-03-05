package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.types.VectorValue

/**
 * L1 or Manhattan distance between to vectors.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
object ManhattanDistance : MinkowskiDistance {
    override val p: Int = 1
    override val cost: Double
        get() = 1.0

    /**
     * Calculates the L1 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): Double = a.distanceL1(b).asDouble().value

    /**
     * Calculates the weighted L1 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights A list of weights
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): Double = ((b-a).abs() * weights).sum().value.toDouble()
}