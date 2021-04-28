package org.vitrivr.cottontail.math.knn.basics

import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 *
 */
interface MinkowskiKernel<T : VectorValue<*>> : DistanceKernel<T> {
    val p: Int
}