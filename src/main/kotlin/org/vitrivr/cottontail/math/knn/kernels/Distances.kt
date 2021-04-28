package org.vitrivr.cottontail.math.knn.kernels

import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.math.knn.basics.WeightedKernel
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A enumeration of all [Distances] supported by Cottontail DB for NNS.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class Distances {
    L1,
    L2,
    L2SQUARED,
    HAMMING,
    COSINE,
    CHISQUARED,
    INNERPRODUCT,
    HAVERSINE;


    /**
     * Calculates and returns the cost for applying the given [Distances] to a vector of dimension [d].
     *
     * @param d The dimension of the query vector.
     * @param weighted Flag indicating, whether a weighted calculation should be conduced.
     * @return Cost for a single operation.
     */
    fun cost(d: Int, weighted: Boolean = false): Float = if (weighted) {
        when (this) {
            L1 -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.ManhattanKernel.cost(d)
            L2 -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.EuclideanKernel.cost(d)
            L2SQUARED -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.SquaredEuclideanKernel.cost(d)
            HAMMING -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.HammingKernel.cost(d)
            COSINE -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.CosineKernel.cost(d)
            CHISQUARED -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.ChisquaredKernel.cost(d)
            else -> TODO()
        }
    } else {
        when (this) {
            L1 -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.ManhattanKernel.cost(d)
            L2 -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.EuclideanKernel.cost(d)
            L2SQUARED -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.SquaredEuclideanKernel.cost(d)
            HAMMING -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.HammingKernel.cost(d)
            COSINE -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.CosineKernel.cost(d)
            CHISQUARED -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.ChisquaredKernel.cost(d)
            INNERPRODUCT -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.InnerProductKernel.cost(d)
            HAVERSINE -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.HaversineKernel.cost(d)
        }
    }

    /**
     * Returns the correct [DistanceKernel] for the given [query] [VectorValue].
     *
     * @param query The query [VectorValue]
     * @return The appropriate [DistanceKernel].
     * @throws KernelNotFoundException If no supported [DistanceKernel] could be found.
     */
    fun kernelForQuery(query: VectorValue<*>): DistanceKernel<*> = when (this) {
        L1 -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.ManhattanKernel.kernel(query)
        L2 -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.EuclideanKernel.kernel(query)
        L2SQUARED -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.SquaredEuclideanKernel.kernel(query)
        HAMMING -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.HammingKernel.kernel(query)
        COSINE -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.CosineKernel.kernel(query)
        CHISQUARED -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.ChisquaredKernel.kernel(query)
        INNERPRODUCT -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.InnerProductKernel.kernel(query)
        HAVERSINE -> org.vitrivr.cottontail.math.knn.kernels.binary.plain.HaversineKernel.kernel(query)
    }

    /**
     * Returns the correct [WeightedKernel] for the given [query] and [weight] [RealVectorValue].
     * [WeightedKernel] only support real [VectorValue]s
     *
     * @param query The [RealVectorValue] used as query
     * @param weight The [RealVectorValue] used as weight
     * @return The appropriate [WeightedKernel].
     * @throws KernelNotFoundException If no supported [DistanceKernel] could be found.
     */
    fun kernelForQueryAndWeight(query: RealVectorValue<*>, weight: RealVectorValue<*>): WeightedKernel<*> = when (this) {
        L1 -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.ManhattanKernel.kernel(query, weight)
        L2 -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.EuclideanKernel.kernel(query, weight)
        L2SQUARED -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.SquaredEuclideanKernel.kernel(query, weight)
        HAMMING -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.HammingKernel.kernel(query, weight)
        COSINE -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.CosineKernel.kernel(query, weight)
        CHISQUARED -> org.vitrivr.cottontail.math.knn.kernels.binary.weighted.ChisquaredKernel.kernel(query, weight)
        else -> TODO()
    }
}