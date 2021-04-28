package org.vitrivr.cottontail.math.knn.kernels.binary.weighted

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.basics.MinkowskiKernel
import org.vitrivr.cottontail.math.knn.basics.WeightedKernel
import org.vitrivr.cottontail.math.knn.kernels.KernelNotFoundException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue

import kotlin.math.absoluteValue

/**
 * A [ManhattanKernel] implementation to calculate Manhattan or L1 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class ManhattanKernel<T : RealVectorValue<*>>(final override val query: T, final override val weight: T) : MinkowskiKernel<T>, WeightedKernel<T> {
    companion object {
        /**
         * Returns the [ManhattanKernel] implementation for the given [query] and [weight] [VectorValue].
         *
         * @param query The [RealVectorValue] to return the [ManhattanKernel] for.
         * @param weight The [RealVectorValue] to return the [ManhattanKernel] for.
         * @return [ManhattanKernel]
         * @throws KernelNotFoundException If no supported kernel could be found.
         */
        fun kernel(query: RealVectorValue<*>, weight: RealVectorValue<*>) = when (query) {
            is DoubleVectorValue -> ManhattanKernel.DoubleVector(query, weight as DoubleVectorValue)
            is FloatVectorValue -> ManhattanKernel.FloatVector(query, weight as FloatVectorValue)
            is IntVectorValue -> ManhattanKernel.IntVector(query, weight as IntVectorValue)
            is LongVectorValue -> ManhattanKernel.LongVector(query, weight as LongVectorValue)
            else -> throw KernelNotFoundException(ManhattanKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [ManhattanKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (3.0f * Cost.COST_FLOP + 3.0f * Cost.COST_MEMORY_ACCESS)
    }

    /** The [p] value for an [ManhattanKernel] instance is always 2. */
    final override val p: Int = 1

    /** The cost of applying this [ManhattanKernel] to a single [VectorValue]. */
    override val cost: Float
        get() = cost(this.d)

    /**
     * [ManhattanKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue, weight: DoubleVectorValue) : ManhattanKernel<DoubleVectorValue>(query, weight) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ManhattanKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue, weight: FloatVectorValue) : ManhattanKernel<FloatVectorValue>(query, weight) {
        override fun invoke(vector: FloatVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ManhattanKernel] for a [LongVectorValue].
     */
    class LongVector(query: LongVectorValue, weight: LongVectorValue) : ManhattanKernel<LongVectorValue>(query, weight) {
        override fun invoke(vector: LongVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ManhattanKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue, weight: IntVectorValue) : ManhattanKernel<IntVectorValue>(query, weight) {
        override fun invoke(vector: IntVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }
}