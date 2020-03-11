package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

enum class Distances (val kernel: DistanceKernel) {
    L1(ManhattanDistance),
    L2(EuclidianDistance),
    L2SQUARED(SquaredEuclidianDistance),
    HAMMING(HammingDistance),
    COSINE(CosineDistance)
}