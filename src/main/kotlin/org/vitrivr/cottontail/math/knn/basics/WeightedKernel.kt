package org.vitrivr.cottontail.math.knn.basics

import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A kernel function used for distance calculations between a query [VectorValue] and other vector values.
 *
 * Supports per-component weights.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface WeightedKernel<T : RealVectorValue<*>> : DistanceKernel<T> {
    /** The weight [VectorValue]. */
    val weight: T
}