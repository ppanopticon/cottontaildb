package org.vitrivr.cottontail.database.index.lsh.superbit

import org.vitrivr.cottontail.model.values.types.ComplexVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.io.Serializable

/**
 * LSH implementation relying on Super-Bit, to bin vectors s times (stages)  in b buckets (per stage),
 * in a space with d dimensions. Input vectors  with a high cosine similarity have a high probability
 * of falling in the same bucket.
 *
 * @param s    stages
 * @param b    buckets (per stage)
 * @param seed random number generator seed (sing the same value will guarantee identical hashes across object
 *             instantiations)
 *
 * This class is inspired by Thibault Debatty (https://github.com/tdebatty/java-LSH).
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.1
 */
class SuperBitLSH(
    private val s: Int,
    private val b: Int,
    seed: Long,
    species: VectorValue<*>,
    private val considerImaginary: Boolean,
    samplingMethod: SamplingMethod
) : Serializable {

    companion object {
        const val LARGE_PRIME: Long = 433494437
    }

    private val k = s * b / 2
    private val N = computeSuperBitDepth(species.logicalSize, k)
    val superBit = SuperBit(N, k / N, seed, samplingMethod, species)

    /**
     * Compute the Super-Bit depth N.
     *
     * @param d The dimension of the vector.
     * @return Super-Bit depth N.
     */
    private fun computeSuperBitDepth(d: Int, k: Int): Int {
        var N: Int = d
        while (N >= 1) {
            if (k % N == 0) {
                break
            }
            N--
        }
        require(N != 0) { "Super-Bit depth must not be 0" }
        return N
    }

    /**
     * Hash a double[] vector in s stages into b buckets.
     *
     * @param vector
     * @return A int[] with the signature.
     */
    fun hash(vector: VectorValue<*>): IntArray {
        return if (this.considerImaginary && vector is ComplexVectorValue) {
            hashSignature(this.superBit.signatureComplex(vector))
        } else {
            hashSignature(superBit.signature(vector))
        }
    }

    /**
     * Hash a signature.
     * The signature is divided in s stages. Each stage is hashed to one of the b buckets.
     *
     * @param signature
     * @return A vector of s integers (between 0 and b - 1)
     */
    private fun hashSignature(
        signatureReal: BooleanArray,
        signatureComplex: BooleanArray? = null
    ): IntArray {
        // create an accumulator for each stage
        // is this hashing locality preserving?
        require(signatureReal.size == k)
        val acc = LongArray(s)
        for (i in 0 until s) {
            acc[i] = 0
        }
        // number of rows per stage
        val rows = signatureReal.size / s
        for (i in signatureReal.indices) {
            var j: Long = 0
            if (signatureReal[i]) j = (i + 1) * LARGE_PRIME
            // todo: Is this modification killing the hamming distance preserving features of this method (if it was there to begin with)?
            if (signatureComplex != null && signatureComplex[i]) j += (i + 1) * LARGE_PRIME
            // current stage
            val k = (i / rows).coerceAtMost(s - 1)
            acc[k] = (acc[k] + j) % Int.MAX_VALUE
        }
        val vector = IntArray(s)
        for (i in 0 until s) {
            vector[i] = acc[i].toInt() % b
        }
        return vector
    }

}