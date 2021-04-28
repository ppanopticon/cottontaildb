package org.vitrivr.cottontail.math.knn.kernels.binary.plain

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.math.knn.kernels.KernelNotFoundException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * A [DistanceKernel] implementation to calculate the Cosine distance between a [query] and a series of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class CosineKernel<T : VectorValue<*>>(final override val query: T) : DistanceKernel<T> {

    companion object {
        /**
         * Returns the [CosineKernel] implementation for the given [VectorValue].
         *
         * @param query [VectorValue] The [VectorValue] to return the [CosineKernel] for.
         * @return [CosineKernel]
         * @throws KernelNotFoundException If no supported kernel could be found.
         */
        fun kernel(query: VectorValue<*>) = when (query) {
            is DoubleVectorValue -> CosineKernel.DoubleVector(query)
            is FloatVectorValue -> CosineKernel.FloatVector(query)
            is IntVectorValue -> CosineKernel.IntVector(query)
            is LongVectorValue -> CosineKernel.LongVector(query)
            else -> throw KernelNotFoundException(CosineKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [CosineKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (6.0f * Cost.COST_FLOP + 4.0f * Cost.COST_MEMORY_ACCESS) + 4.0f * Cost.COST_FLOP + 3.0f * Cost.COST_MEMORY_ACCESS
    }

    /** The cost of applying this [CosineKernel] to a single [VectorValue]. */
    override val cost: Float
        get() = cost(this.d)


    /**
     * [CosineKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue) : CosineKernel<DoubleVectorValue>(query) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue {
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i])
                normq += this.query.data[i].pow(2)
                normv += vector.data[i].pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue) : CosineKernel<FloatVectorValue>(query) {
        override fun invoke(vector: FloatVectorValue): DoubleValue {
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i])
                normq += this.query.data[i].pow(2)
                normv += vector.data[i].pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineKernel] for a [LongVectorValue].
     */
    class LongVector(d: LongVectorValue) : CosineKernel<LongVectorValue>(d) {
        override fun invoke(vector: LongVectorValue): DoubleValue {
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i])
                normq += this.query.data[i].toDouble().pow(2)
                normv += vector.data[i].toDouble().pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue) : CosineKernel<IntVectorValue>(query) {
        override fun invoke(vector: IntVectorValue): DoubleValue {
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i])
                normq += this.query.data[i].toDouble().pow(2)
                normv += vector.data[i].toDouble().pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }
}