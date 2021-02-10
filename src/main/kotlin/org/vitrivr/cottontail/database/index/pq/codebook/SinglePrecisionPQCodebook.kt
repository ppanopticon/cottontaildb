package org.vitrivr.cottontail.database.index.pq.codebook

import org.apache.commons.math3.stat.correlation.Covariance
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.database.index.pq.codebook.PQCodebook.Companion.clusterRealData
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A [PQCodebook] implementation for [FloatVectorValue]s (single precision).
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
class SinglePrecisionPQCodebook(
    protected val centroids: List<FloatVectorValue>,
    protected val covMatrix: List<FloatVectorValue>
) : PQCodebook<FloatVectorValue> {

    /** Internal buffer for calculation of diff in [quantizeSubspaceForVector] calculation. */
    private val diffBuffer = FloatVectorValue(FloatArray(this.logicalSize))

    /**
     * Serializer object for [SinglePrecisionPQCodebook]
     */
    object Serializer : org.mapdb.Serializer<SinglePrecisionPQCodebook> {
        override fun serialize(out: DataOutput2, value: SinglePrecisionPQCodebook) {
            /* Serialize logical size of codebook entries. */
            out.packInt(value.logicalSize)
            val vectorSerializer = value.type.serializer()

            /* Serialize centroids matrix. */
            out.packInt(value.centroids.size)
            for (v in value.centroids) {
                vectorSerializer.serialize(out, v)
            }

            /* Serialize covariance matrix. */
            out.packInt(value.covMatrix.size)
            for (v in value.covMatrix) {
                vectorSerializer.serialize(out, v)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): SinglePrecisionPQCodebook {
            val logicalSize = input.unpackInt()
            val vectorSerializer = Type.FloatVector(logicalSize).serializer()
            val centroidsSize = input.unpackInt()
            val centroids = ArrayList<FloatVectorValue>(centroidsSize)
            for (i in 0 until centroidsSize) {
                centroids.add(vectorSerializer.deserialize(input, available))
            }
            val covMatrixSize = input.unpackInt()
            val covMatrix = ArrayList<FloatVectorValue>(covMatrixSize)
            for (i in 0 until covMatrixSize) {
                covMatrix.add(vectorSerializer.deserialize(input, available))
            }
            return SinglePrecisionPQCodebook(centroids, covMatrix)
        }

        override fun isTrusted(): Boolean = true
    }

    companion object {
        /**
         * Learns the [PQCodebook] and the [PQShortSignature] and returns them. Internally, the clustering
         * is done with apache commons k-means++ in double precision but the returned codebook contains
         * centroids of the same datatype as was supplied.
         *
         * @param subspaceData The subspace, i.e., the vectors per subspace.
         * @param numCentroids The number of centroids to learn.
         * @param seed The random seed used for learning.
         * @param maxIterations The number of iterations to use.s
         */
        fun learnFromData(
            subspaceData: List<FloatVectorValue>,
            numCentroids: Int,
            seed: Long,
            maxIterations: Int
        ): SinglePrecisionPQCodebook {
            /* Prepare covariance matrix and centroid clusters. */
            val array = Array(subspaceData.size) { i ->
                DoubleArray(subspaceData[i].data.size) { j ->
                    subspaceData[i].data[j].toDouble()
                }
            }
            val covMatrix = Covariance(array, false).covarianceMatrix
            val centroidClusters =
                clusterRealData(array, covMatrix, numCentroids, seed, maxIterations)

            /* Convert to typed centroids and covariance matrix. */
            val centroids = ArrayList<FloatVectorValue>(numCentroids)
            for (i in 0 until numCentroids) {
                centroids.add(FloatVectorValue(centroidClusters[i].center.point))
            }
            val dataCovMatrix = covMatrix.data.map { FloatVectorValue(it) }

            /* Return PQCodebook. */
            return SinglePrecisionPQCodebook(centroids, dataCovMatrix)
        }
    }

    /** The [SinglePrecisionPQCodebook] handles [FloatVectorValue]s. */
    override val type: Type<FloatVectorValue>
        get() = Type.FloatVector(this.logicalSize)

    /** The number of centroids contained in this [SinglePrecisionPQCodebook]. */
    override val numberOfCentroids: Int
        get() = this.centroids.size

    /** The logical size of the centroids held by this [SinglePrecisionPQCodebook]. */
    override val logicalSize: Int
        get() = this.centroids[0].logicalSize

    /**
     * Returns the centroid [VectorValue] for the given index.
     *
     * @param ci The index of the centroid to return.
     * @return The [DoubleVectorValue] representing the centroid for the given index.
     */
    override fun get(ci: Int): FloatVectorValue = this.centroids[ci]

    /**
     * Quantizes the given [FloatVectorValue] and returns the index of the centroid it belongs to. Distance
     * calculation starts from the given [start] vector component and considers [logicalSize] components.
     *
     * Due to internal optimizations, parallel invocation of this method is not possible (i.e. method is not thread safe).
     *
     * @param v The [VectorValue] to quantize.
     * @param start The index of the first [VectorValue] component to consider for distance calculation.
     * @return The index of the centroid the given [VectorValue] belongs to.
     */
    override fun quantizeSubspaceForVector(v: FloatVectorValue, start: Int): Int {
        var mahIndex = 0
        var mah = Float.POSITIVE_INFINITY
        var i = 0
        outer@ for (c in this.centroids) {
            var dist = 0.0f
            for (it in this.diffBuffer.data.indices) {
                this.diffBuffer.data[it] = c.data[it] - v.data[start + it]
            }
            var j = 0
            for (m in this.covMatrix) {
                dist = Math.fma(this.diffBuffer.data[j++], (m.dot(this.diffBuffer).value), dist)
                if (dist >= mah) {
                    i++
                    continue@outer
                }
            }
            if (dist < mah) {
                mah = dist
                mahIndex = i
            }
            i++
        }
        return mahIndex
    }
}