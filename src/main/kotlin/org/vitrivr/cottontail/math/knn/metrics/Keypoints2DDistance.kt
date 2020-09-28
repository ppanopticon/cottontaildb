package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * Calculates the Keypoints 2D distance between two vectors.
 * This is the sum of Euclidian distances between consecutive pairs of points in a vector
 *
 * @version 1.0
 * @author Frankie Robertson
 */
object Keypoints2DDistance : DistanceKernel {
    override val cost = 1.0f

    private fun procDiffs(diffs: RealVectorValue<*>): DoubleValue {
        val sqDiffs = diffs.pow(2)
        var result = DoubleValue.ZERO
        for (diffIdxs in sqDiffs.indices.chunked(2)) {
            result += (sqDiffs[diffIdxs[0]] + sqDiffs[diffIdxs[1]]).sqrt()
        }
        return result
    }

    /**
     * Calculates the Keypoints 2D distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue {
        if (a !is RealVectorValue || b !is RealVectorValue) {
            throw ArrayConfigurationNotSupportedException("Keypoints2DDistance only supports RealVectorValues");
        }
        if (a.logicalSize % 2 != 0) {
            throw ArrayConfigurationNotSupportedException("Keypoints2DDistance only supports even length vectors");
        }
        return procDiffs((a - b) as RealVectorValue<*>)
    }

    /**
     * Calculates the weighted Hamming distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights A list of weights
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue {
        if (a !is RealVectorValue || b !is RealVectorValue) {
            throw ArrayConfigurationNotSupportedException("Keypoints2DDistance only supports RealVectorValues")
        }
        if (a.logicalSize % 2 != 0) {
            throw ArrayConfigurationNotSupportedException("Keypoints2DDistance only supports even length vectors")
        }
        return procDiffs((weights * (a - b)) as RealVectorValue<*>)
    }
}
