package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

interface MinkowskiDistance: DistanceKernel {
    val p: Int
}