package ch.unibas.dmi.dbis.cottontail.database.index.lsh.superbit

import ch.unibas.dmi.dbis.cottontail.model.values.*
import ch.unibas.dmi.dbis.cottontail.model.values.types.NumericValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.VectorValue
import java.io.Serializable
import java.util.*

/**
 * Implementation of Super-Bit Locality-Sensitive Hashing.
 * Super-Bit is an improvement of Random Projection LSH. It computes an estimation of cosine similarity.
 * <p>
 * Super-Bit Locality-Sensitive Hashing
 * Jianqiu Ji, Jianmin Li, Shuicheng Yan, Bo Zhang, Qi Tian
 * http://papers.nips.cc/paper/4847-super-bit-locality-sensitive-hashing.pdf
 * <p>
 * This class is inspired by Thibault Debatty (https://github.com/tdebatty/java-LSH).
 *
 * @param d     dimension, R^n
 * @param N     Super-Bit depth (must be [1 .. d])
 * @param L     number of Super-Bits (must be [1 ..)
 * @param seed  to use for the random number generator
 * @param species A species of the [VectorValue] for which hyperplanes should be generated.
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.1
 */
class SuperBit(d: Int, N: Int, L: Int, seed: Long, species: VectorValue<*>) : Serializable {

    /** List of hyperplanes held by this [SuperBit]. */
    private val hyperplanes: Array<VectorValue<*>>

    /**
     * The K vectors are orthogonalized in L batches of N vectors.
     * <p>
     * Super-Bit depth N must be [1 .. d]
     * Number of Super-Bits L must be [1 ..
     * The resulting code length is K = N * L (size of signature)
     *
     * @param d         dimension, R^n
     * @param N         Super-Bit depth (must be [1 .. d])
     * @param L         number of Super-Bits (must be [1 ..)
     * @param random    to use for the random number generator
     */

    init {
        require(d > 0) { "Dimension d must be >= 1" }
        require(!(N < 1 || N > d)) { "Super-Bit depth N must be 1 <= N <= d" }
        require(L >= 1) { "Number of Super-Bits L must be >= 1" }

        /*
        -----------------------------------------------------------------------------------------------
        Algorithm 1 - Generating Super-Bit Locality-Sensitive Hashing Projection Vectors
        -----------------------------------------------------------------------------------------------
        Input: Data space dimension d, Super-Bit depth 1 <= N <= d, number of Super-Bit L >= 1,
        resulting code length K = N * L.

        Generate a random matrix H with each element sampled independently from the normal distribution
        N (0, 1), with each column normalized to unit length. Denote H = [v1, v2, ..., vK].

        Output: H ̃ = [w ,w ,...,w ].
        */

        val random = SplittableRandom(seed)
        val K = N * L
        val v = Array(K) {
            val rnd = when(species) {
                is DoubleVectorValue -> DoubleVectorValue.random(species.logicalSize, random)
                is FloatVectorValue -> FloatVectorValue.random(species.logicalSize, random)
                is Complex32VectorValue -> Complex32VectorValue.random(species.logicalSize, random)
                is Complex64VectorValue -> Complex64VectorValue.random(species.logicalSize, random)
                else -> throw IllegalArgumentException("")
            } as VectorValue<*>
            rnd / norm(rnd)
        } // H

        val w = Array(K) { species.copy()  }

        for (i in 0 until L) {
            for (j in 1..N) {
                System.arraycopy(v[i * N + j - 1], 0, w[i * N + j - 1], 0, d * 2)
                for (k in 1 until j) {
                    w[i * N + j - 1] = w[i * N + j - 1] - (w[i * N + k - 1] * dotProduct(w[i * N + k - 1], v[i * N + j - 1]))
                }
                w[i * N + j - 1] = w[i * N + j - 1] / norm(w[i * N + j - 1])
            }
        }

        this.hyperplanes = w // H ̃
    }

    /**
     * Compute the signature of a vector.
     *
     * @param vector
     * @return The signature.
     */
    fun signature(vector: VectorValue<*>): BooleanArray {
        val signature = BooleanArray(this.hyperplanes.size)
        for (i in hyperplanes.indices) {
            signature[i] = dotProduct(this.hyperplanes[i], vector) >= 0
        }
        return signature
    }

    /**
     * Calculate the norm L2 (sqrt(sum_i(v_i^2))).
     *
     * @param vector
     * @return The norm L2.
     */
    private fun norm(vector: VectorValue<*>): NumericValue<*> = (vector.pow(2).sum()).sqrt()

    /**
     * Calculate the dot product of two vectors.
     *
     * @param vector1
     * @param vector2
     * @return The dot product.
     */
    private fun dotProduct(vector1: VectorValue<*>, vector2: VectorValue<*>): NumericValue<*> = (vector1 * vector2).sum()
}