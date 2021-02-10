package org.vitrivr.cottontail.database.index.lsh.superbit

import org.vitrivr.cottontail.model.values.Complex32VectorValue
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.model.values.types.ComplexVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
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
 * @param N     Super-Bit depth (must be [1 .. d])
 * @param L     number of Super-Bits (must be [1 ..)
 * @param seed  to use for the random number generator
 * @param species A species of the [VectorValue] for which hyperplanes should be generated.
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.1
 */
class SuperBit(
    val N: Int,
    val L: Int,
    val seed: Long,
    val samplingMethod: SamplingMethod,
    species: VectorValue<*>
) : Serializable {

    /** The logical size of the species [VectorValue]. */
    val d = species.logicalSize

    /** List of hyperplanes held by this [SuperBit]. */
    private val _hyperplanes: Array<VectorValue<*>>
    val hyperplanes
        get() = _hyperplanes.copyOf()

    /**
     * The K vectors are orthogonalized in L batches of N vectors.
     * <p>
     * Super-Bit depth N must be [1 .. d]
     * Number of Super-Bits L must be [1 ..
     * The resulting code length is K = N * L (size of signature)
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

        val K = N * L
        val v = when (samplingMethod) {
            SamplingMethod.UNIFORM -> {
                val random = SplittableRandom(seed)
                Array(K) {
                    val rnd =
                        when (species) { // centering components at 0 (subtract 0.5) looks more favorable in terms of hamming dist vs. cosine dist plots
                            is DoubleVectorValue -> DoubleVectorValue(DoubleArray(species.logicalSize) { random.nextDouble() - 0.5 })
                            is FloatVectorValue -> FloatVectorValue(FloatArray(species.logicalSize) { (random.nextDouble() - 0.5).toFloat() })
                            is Complex32VectorValue -> Complex32VectorValue(FloatArray(species.logicalSize * 2) { (random.nextDouble() - 0.5).toFloat() })
                            is Complex64VectorValue -> Complex64VectorValue(DoubleArray(species.logicalSize * 2) { random.nextDouble() - 0.5 })
                            else -> throw IllegalArgumentException("Unsupported vector type")
                        } as VectorValue<*>
                    rnd / rnd.norm2()
                } // H
            }
            SamplingMethod.GAUSSIAN -> {
                val random = Random(seed)
                Array(K) {
                    val rnd = when (species) {
                        is DoubleVectorValue -> DoubleVectorValue(DoubleArray(species.logicalSize) { random.nextGaussian() })
                        is FloatVectorValue -> FloatVectorValue(FloatArray(species.logicalSize) {
                            random.nextGaussian().toFloat()
                        })
                        is Complex32VectorValue -> Complex32VectorValue(FloatArray(species.logicalSize * 2) {
                            random.nextGaussian().toFloat()
                        })
                        is Complex64VectorValue -> Complex64VectorValue(DoubleArray(species.logicalSize * 2) { random.nextGaussian() })
                        else -> throw IllegalArgumentException("Unsupported vector type")
                    } as VectorValue<*>
                    rnd / rnd.norm2()
                }
            }
        }

        val w = Array(K) { v[it] }

        for (i in 0 until L) {
            for (j in 1..N) {
                for (k in 1 until j) {
                    w[i * N + j - 1] =
                        w[i * N + j - 1] - (w[i * N + k - 1] * w[i * N + j - 1].dot(v[i * N + k - 1])) // order of dot product matters for complex vectors!
                }
                w[i * N + j - 1] = w[i * N + j - 1] / w[i * N + j - 1].norm2()
            }
        }

        this._hyperplanes = w // H ̃
    }

    /**
     * Compute the signature of a vector.
     *
     * @param vector
     * @return The signature.
     */
    fun signature(vector: VectorValue<*>): BooleanArray {
        val signature = BooleanArray(this._hyperplanes.size)
        for (i in this._hyperplanes.indices) {
            signature[i] = this._hyperplanes[i].dot(vector).asInt().value >= 0
        }
        return signature
    }

    /**
     * Compute the signature of a complex vector. Take every even hyperplane for the real part, every odd one for the imaginary part.
     *
     * todo: are there better ways to incorporate the imaginary part?
     *       if in the end the distance measure is the absolute IP, could we maybe do something like this here?
     *
     * @param vector
     * @return The signature.
     */
    fun signatureComplex(vector: ComplexVectorValue<*>): BooleanArray {
        return BooleanArray(this._hyperplanes.size) {
            if (it % 2 == 0) {
                this._hyperplanes[it].dot(vector).real.asInt().value >= 0
            } else {
                this._hyperplanes[it].dot(vector).imaginary.asInt().value >= 0
            }
        }
    }
}