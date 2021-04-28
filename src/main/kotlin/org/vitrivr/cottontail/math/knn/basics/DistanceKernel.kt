package org.vitrivr.cottontail.math.knn.basics

import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A kernel function used for distance calculations between a query [VectorValue] and other vector values.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
interface DistanceKernel<T : VectorValue<*>> {
    /** The query [VectorValue]. */
    val query: T

    /** The dimensionality of this [DistanceKernel]. */
    val d: Int
        get() = this.query.logicalSize

    /**
     * Estimates of the cost incurred by applying this [DistanceKernel] to a [VectorValue] of size [d].
     *
     * @param d The dimension to calculate the cost for.
     * @return The estimated cost.
     */
    val cost: Float

    /**
     * Calculates the distance between this [query] [VectorValue]s and the given [VectorValue].
     *
     * @param vector First [VectorValue]
     * @return Distance between [query] and [vector].
     */
    operator fun invoke(vector: T): DoubleValue
}