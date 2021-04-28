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
 * A [EuclideanKernel] implementation to calculate the Euclidean or L2 distance between a [query] and a series of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class EuclideanKernel<T : RealVectorValue<*>>(final override val query: T, final override val weight: T) : MinkowskiKernel<T>, WeightedKernel<T> {

    companion object {
        /**
         * Returns the [EuclideanKernel] implementation for the given [query] and [weight] [VectorValue].
         *
         * @param query The [RealVectorValue] to return the [EuclideanKernel] for.
         * @param weight The [RealVectorValue] to return the [EuclideanKernel] for.
         * @return [EuclideanKernel]
         * @throws KernelNotFoundException If no supported kernel could be found.
         */
        fun kernel(query: RealVectorValue<*>, weight: RealVectorValue<*>) = when (query) {
            is DoubleVectorValue -> EuclideanKernel.DoubleVector(query, weight as DoubleVectorValue)
            is FloatVectorValue -> EuclideanKernel.FloatVector(query, weight as FloatVectorValue)
            is IntVectorValue -> EuclideanKernel.IntVector(query, weight as IntVectorValue)
            is LongVectorValue -> EuclideanKernel.LongVector(query, weight as LongVectorValue)
            else -> throw KernelNotFoundException(EuclideanKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [EuclideanKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (4.0f * Cost.COST_FLOP + 3.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP + Cost.COST_MEMORY_ACCESS
    }

    /** The [p] value for an [EuclideanKernel] instance is always 2. */
    final override val p: Int = 2

    /** The cost of applying this [EuclideanKernel] to a single [VectorValue]. */
    override val cost: Float
        get() = cost(this.d)

    /**
     * [EuclideanKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue, weight: DoubleVectorValue) : EuclideanKernel<DoubleVectorValue>(query, weight) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2) * this.weight.data[i]
            }
            return DoubleValue(kotlin.math.sqrt(sum))
        }
    }

    /**
     * [EuclideanKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue, weight: FloatVectorValue) : EuclideanKernel<FloatVectorValue>(query, weight) {
        override fun invoke(vector: FloatVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2) * this.weight.data[i]
            }
            return DoubleValue(kotlin.math.sqrt(sum))
        }
    }

    /**
     * [EuclideanKernel] for a [LongVectorValue].
     */
    class LongVector(query: LongVectorValue, weight: LongVectorValue) : EuclideanKernel<LongVectorValue>(query, weight) {
        override fun invoke(vector: LongVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).toDouble().pow(2) * this.weight.data[i]
            }
            return DoubleValue(kotlin.math.sqrt(sum))
        }
    }

    /**
     * [EuclideanKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue, weight: IntVectorValue) : EuclideanKernel<IntVectorValue>(query, weight) {
        override fun invoke(vector: IntVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).toDouble().pow(2) * this.weight.data[i]
            }
            return DoubleValue(kotlin.math.sqrt(sum))
        }
    }
}