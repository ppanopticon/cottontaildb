package org.vitrivr.cottontail.math.knn.kernels.binary.weighted

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.math.knn.basics.WeightedKernel
import org.vitrivr.cottontail.math.knn.kernels.KernelNotFoundException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.pow

/**
 * A [DistanceKernel] implementation to calculate the Chi^2 distance between a [query] and a series of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class ChisquaredKernel<T : RealVectorValue<*>>(final override val query: T, final override val weight: T) : DistanceKernel<T>, WeightedKernel<T> {

    companion object {
        /**
         * Returns the [ChisquaredKernel] implementation for the given [query] and [weight] [VectorValue].
         *
         * @param query The [RealVectorValue] to return the [ChisquaredKernel] for.
         * @param weight The [RealVectorValue] to return the [ChisquaredKernel] for.
         * @return [ChisquaredKernel]
         * @throws KernelNotFoundException If no supported kernel could be found.
         */
        fun kernel(query: RealVectorValue<*>, weight: RealVectorValue<*>) = when (query) {
            is DoubleVectorValue -> ChisquaredKernel.DoubleVector(query, weight as DoubleVectorValue)
            is FloatVectorValue -> ChisquaredKernel.FloatVector(query, weight as FloatVectorValue)
            is IntVectorValue -> ChisquaredKernel.IntVector(query, weight as IntVectorValue)
            is LongVectorValue -> ChisquaredKernel.LongVector(query, weight as LongVectorValue)
            else -> throw KernelNotFoundException(ChisquaredKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [ChisquaredKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (6.0f * Cost.COST_FLOP + 5.0f * Cost.COST_MEMORY_ACCESS)
    }

    /** The cost of applying this [ChisquaredKernel] to a single [VectorValue]. */
    override val cost: Float
        get() = cost(this.d)

    /**
     * [ChisquaredKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue, weight: DoubleVectorValue) : ChisquaredKernel<DoubleVectorValue>(query, weight) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (((this.query.data[i] - vector.data[i]).pow(2)) / (this.query.data[i] + vector.data[i])) * this.weight.data[i]
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ChisquaredKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue, weight: FloatVectorValue) : ChisquaredKernel<FloatVectorValue>(query, weight) {
        override fun invoke(vector: FloatVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (((this.query.data[i] - vector.data[i]).pow(2)) / (this.query.data[i] + vector.data[i])) * this.weight.data[i]
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ChisquaredKernel] for a [LongVectorValue].
     */
    class LongVector(query: LongVectorValue, weight: LongVectorValue) : ChisquaredKernel<LongVectorValue>(query, weight) {
        override fun invoke(vector: LongVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (((this.query.data[i] - vector.data[i]).toDouble().pow(2)) / (this.query.data[i] + vector.data[i])) * this.weight.data[i]
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ChisquaredKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue, weight: IntVectorValue) : ChisquaredKernel<IntVectorValue>(query, weight) {
        override fun invoke(vector: IntVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (((this.query.data[i] - vector.data[i]).toDouble().pow(2)) / (this.query.data[i] + vector.data[i])) * this.weight.data[i]
            }
            return DoubleValue(sum)
        }
    }
}