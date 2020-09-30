package org.vitrivr.cottontail.math.knn.metrics

interface MinkowskiDistance: DistanceKernel {
    val p: Int
}