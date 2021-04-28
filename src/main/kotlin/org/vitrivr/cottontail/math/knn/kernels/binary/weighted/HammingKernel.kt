package org.vitrivr.cottontail.math.knn.kernels.binary.weighted

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.math.knn.basics.WeightedKernel
import org.vitrivr.cottontail.math.knn.kernels.KernelNotFoundException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A [DistanceKernel] implementation to calculate the Hamming distance between a [query] and a series of [VectorValue]s.

 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class HammingKernel<T : RealVectorValue<*>>(final override val query: T, final override val weight: T) : DistanceKernel<T>, WeightedKernel<T> {

    companion object {
        /**
         * Returns the [HammingKernel] implementation for the given [query] and [weight] [VectorValue].
         *
         * @param query The [RealVectorValue] to return the [HammingKernel] for.
         * @param weight The [RealVectorValue] to return the [HammingKernel] for.
         * @return [HammingKernel]
         * @throws KernelNotFoundException If no supported kernel could be found.
         */
        fun kernel(query: RealVectorValue<*>, weight: RealVectorValue<*>) = when (query) {
            is DoubleVectorValue -> HammingKernel.DoubleVector(query, weight as DoubleVectorValue)
            is FloatVectorValue -> HammingKernel.FloatVector(query, weight as FloatVectorValue)
            is IntVectorValue -> HammingKernel.IntVector(query, weight as IntVectorValue)
            is LongVectorValue -> HammingKernel.LongVector(query, weight as LongVectorValue)
            else -> throw KernelNotFoundException(HammingKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [HammingKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS)
    }

    override val cost: Float
        get() = cost(this.d)

    /**
     * [HammingKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue, weight: DoubleVectorValue) : HammingKernel<DoubleVectorValue>(query, weight) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                if (this.query.data[i] != vector.data[i]) {
                    sum += this.weight.data[i]
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue, weight: FloatVectorValue) : HammingKernel<FloatVectorValue>(query, weight) {
        override fun invoke(vector: FloatVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                if (this.query.data[i] != vector.data[i]) {
                    sum += this.weight.data[i]
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingKernel] for a [LongVectorValue].
     */
    class LongVector(d: LongVectorValue, weight: LongVectorValue) : HammingKernel<LongVectorValue>(d, weight) {
        override fun invoke(vector: LongVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                if (this.query.data[i] != vector.data[i]) {
                    sum += this.weight.data[i]
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue, weight: IntVectorValue) : HammingKernel<IntVectorValue>(query, weight) {
        override fun invoke(vector: IntVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                if (this.query.data[i] != vector.data[i]) {
                    sum += this.weight.data[i]
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingKernel] for a [BooleanVectorValue].
     */
    class BooleanVector(query: BooleanVectorValue, weight: BooleanVectorValue) : HammingKernel<BooleanVectorValue>(query, weight) {
        override fun invoke(vector: BooleanVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                if (this.query.data[i] != vector.data[i]) {
                    sum += if (this.weight.data[i]) {
                        1.0
                    } else {
                        0.0
                    }
                }
            }
            return DoubleValue(sum)
        }
    }
}