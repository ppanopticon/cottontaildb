package org.vitrivr.cottontail.math.knn.kernels.binary.weighted

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.basics.MinkowskiKernel
import org.vitrivr.cottontail.math.knn.basics.WeightedKernel
import org.vitrivr.cottontail.math.knn.kernels.KernelNotFoundException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.pow

/**
 * A [MinkowskiKernel] implementation to calculate the Squared Euclidean or squared L2 distance between a [query] and a series of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class SquaredEuclideanKernel<T : RealVectorValue<*>>(final override val query: T, final override val weight: T) : MinkowskiKernel<T>, WeightedKernel<T> {
    companion object {
        /**
         * Returns the [SquaredEuclideanKernel] implementation for the given [query] and [weight] [VectorValue].
         *
         * @param query The [RealVectorValue] to return the [SquaredEuclideanKernel] for.
         * @param weight The [RealVectorValue] to return the [SquaredEuclideanKernel] for.
         * @return [SquaredEuclideanKernel]
         * @throws KernelNotFoundException If no supported kernel could be found.
         */
        fun kernel(query: RealVectorValue<*>, weight: RealVectorValue<*>) = when (query) {
            is DoubleVectorValue -> SquaredEuclideanKernel.DoubleVector(query, weight as DoubleVectorValue)
            is FloatVectorValue -> SquaredEuclideanKernel.FloatVector(query, weight as FloatVectorValue)
            is IntVectorValue -> SquaredEuclideanKernel.IntVector(query, weight as IntVectorValue)
            is LongVectorValue -> SquaredEuclideanKernel.LongVector(query, weight as LongVectorValue)
            else -> throw KernelNotFoundException(SquaredEuclideanKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [SquaredEuclideanKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (4.0f * Cost.COST_FLOP + 3.0f * Cost.COST_MEMORY_ACCESS)
    }

    /** The [p] value for an [SquaredEuclideanKernel] instance is always 2. */
    final override val p: Int = 2

    /** The cost of applying this [SquaredEuclideanKernel] to a single [VectorValue]. */
    override val cost: Float
        get() = cost(this.d)

    /**
     * [SquaredEuclideanKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue, weight: DoubleVectorValue) : SquaredEuclideanKernel<DoubleVectorValue>(query, weight) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2) * this.weight.data[i]
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue, weight: FloatVectorValue) : SquaredEuclideanKernel<FloatVectorValue>(query, weight) {
        override fun invoke(vector: FloatVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2) * this.weight.data[i]
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanKernel] for a [LongVectorValue].
     */
    class LongVector(query: LongVectorValue, weight: LongVectorValue) : SquaredEuclideanKernel<LongVectorValue>(query, weight) {
        override fun invoke(vector: LongVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).toDouble().pow(2) * this.weight.data[i]
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue, weight: IntVectorValue) : SquaredEuclideanKernel<IntVectorValue>(query, weight) {
        override fun invoke(vector: IntVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).toDouble().pow(2) * this.weight.data[i]
            }
            return DoubleValue(sum)
        }
    }
}