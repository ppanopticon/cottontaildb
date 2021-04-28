package org.vitrivr.cottontail.math.knn.kernels.binary.weighted

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.math.knn.basics.WeightedKernel
import org.vitrivr.cottontail.math.knn.kernels.KernelNotFoundException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * A [DistanceKernel] implementation to calculate the weighted  Cosine distance between a [query] and a series of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class CosineKernel<T : RealVectorValue<*>>(final override val query: T, final override val weight: T) : DistanceKernel<T>, WeightedKernel<T> {

    companion object {
        /**
         * Returns the [CosineKernel] implementation for the given [query] and [weight] [VectorValue].
         *
         * @param query The [RealVectorValue] to return the [CosineKernel] for.
         * @param weight The [RealVectorValue] to return the [CosineKernel] for.
         * @return [CosineKernel]
         * @throws KernelNotFoundException If no supported kernel could be found.
         */
        fun kernel(query: RealVectorValue<*>, weight: RealVectorValue<*>) = when (query) {
            is DoubleVectorValue -> CosineKernel.DoubleVector(query, weight as DoubleVectorValue)
            is FloatVectorValue -> CosineKernel.FloatVector(query, weight as FloatVectorValue)
            is IntVectorValue -> CosineKernel.IntVector(query, weight as IntVectorValue)
            is LongVectorValue -> CosineKernel.LongVector(query, weight as LongVectorValue)
            else -> throw KernelNotFoundException(CosineKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [CosineKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (9.0f * Cost.COST_FLOP + 7.0f * Cost.COST_MEMORY_ACCESS) + 4.0f * Cost.COST_FLOP + 3.0f * Cost.COST_MEMORY_ACCESS
    }

    /** The cost of applying this [CosineKernel] to a single [VectorValue]. */
    override val cost: Float
        get() = cost(this.d)

    /**
     * [CosineKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue, weight: DoubleVectorValue) : CosineKernel<DoubleVectorValue>(query, weight) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue {
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i]) * this.weight.data[i]
                normq += this.query.data[i].pow(2) * this.weight.data[i]
                normv += vector.data[i].pow(2) * this.weight.data[i]
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue, weight: FloatVectorValue) : CosineKernel<FloatVectorValue>(query, weight) {
        override fun invoke(vector: FloatVectorValue): DoubleValue {
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i]) * this.weight.data[i]
                normq += this.query.data[i].pow(2) * this.weight.data[i]
                normv += vector.data[i].pow(2) * this.weight.data[i]
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineKernel] for a [LongVectorValue].
     */
    class LongVector(query: LongVectorValue, weight: LongVectorValue) : CosineKernel<LongVectorValue>(query, weight) {
        override fun invoke(vector: LongVectorValue): DoubleValue {
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i]) * this.weight.data[i]
                normq += this.query.data[i].toDouble().pow(2) * this.weight.data[i]
                normv += vector.data[i].toDouble().pow(2) * this.weight.data[i]
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue, weight: IntVectorValue) : CosineKernel<IntVectorValue>(query, weight) {
        override fun invoke(vector: IntVectorValue): DoubleValue {
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i]) * this.weight.data[i]
                normq += this.query.data[i].toDouble().pow(2) * this.weight.data[i]
                normv += vector.data[i].toDouble().pow(2) * this.weight.data[i]
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }
}