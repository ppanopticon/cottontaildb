package org.vitrivr.cottontail.math.knn.kernels.binary.plain

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.math.knn.kernels.KernelNotFoundException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A [DistanceKernel] implementation to calculate the Hamming distance between a [query] and a series of [VectorValue]s.

 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class HammingKernel<T : VectorValue<*>>(final override val query: T) : DistanceKernel<T> {
    companion object {
        /**
         * Returns the [HammingKernel] implementation for the given [VectorValue].
         *
         * @param query [VectorValue] The [VectorValue] to return the [HammingKernel] for.
         * @return [HammingKernel]
         * @throws KernelNotFoundException If no supported [HammingKernel] implementation could not be found.
         */
        fun kernel(query: VectorValue<*>) = when (query) {
            is DoubleVectorValue -> HammingKernel.DoubleVector(query)
            is FloatVectorValue -> HammingKernel.FloatVector(query)
            is IntVectorValue -> HammingKernel.IntVector(query)
            is LongVectorValue -> HammingKernel.LongVector(query)
            is BooleanVectorValue -> HammingKernel.BooleanVector(query)
            else -> throw KernelNotFoundException(HammingKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [HammingKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (Cost.COST_FLOP + Cost.COST_MEMORY_ACCESS)
    }

    override val cost: Float
        get() = cost(this.d)

    /**
     * [HammingKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue) : HammingKernel<DoubleVectorValue>(query) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                if (this.query.data[i] != vector.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue) : HammingKernel<FloatVectorValue>(query) {
        override fun invoke(vector: FloatVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                if (this.query.data[i] != vector.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingKernel] for a [LongVectorValue].
     */
    class LongVector(d: LongVectorValue) : HammingKernel<LongVectorValue>(d) {
        override fun invoke(vector: LongVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                if (this.query.data[i] != vector.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue) : HammingKernel<IntVectorValue>(query) {
        override fun invoke(vector: IntVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                if (this.query.data[i] != vector.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingKernel] for a [BooleanVectorValue].
     */
    class BooleanVector(query: BooleanVectorValue) : HammingKernel<BooleanVectorValue>(query) {
        override fun invoke(vector: BooleanVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                if (this.query.data[i] != vector.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
    }
}