package org.vitrivr.cottontail.math.knn.kernels.binary.plain

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.basics.MinkowskiKernel
import org.vitrivr.cottontail.math.knn.kernels.KernelNotFoundException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.pow

/**
 * A [EuclideanKernel] implementation to calculate the Euclidean or L2 distance between a [query] and a series of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class EuclideanKernel<T : VectorValue<*>>(final override val query: T) : MinkowskiKernel<T> {

    companion object {
        /**
         * Returns the [EuclideanKernel] implementation for the given [VectorValue].
         *
         * @param query [VectorValue] The [VectorValue] to return the [EuclideanKernel] for.
         * @return [EuclideanKernel]
         * @throws KernelNotFoundException If no supported [EuclideanKernel] could not be found.
         */
        fun kernel(query: VectorValue<*>) = when (query) {
            is Complex64VectorValue -> EuclideanKernel.Complex64Vector(query)
            is Complex32VectorValue -> EuclideanKernel.Complex32Vector(query)
            is DoubleVectorValue -> EuclideanKernel.DoubleVector(query)
            is FloatVectorValue -> EuclideanKernel.FloatVector(query)
            is IntVectorValue -> EuclideanKernel.IntVector(query)
            is LongVectorValue -> EuclideanKernel.LongVector(query)
            else -> throw KernelNotFoundException(EuclideanKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [EuclideanKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (3.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP + Cost.COST_MEMORY_ACCESS
    }

    /** The [p] value for an [EuclideanKernel] instance is always 2. */
    final override val p: Int = 2

    /** The cost of applying this [EuclideanKernel] to a single [VectorValue]. */
    override val cost: Float
        get() = cost(this.d)

    /**
     * [EuclideanKernel] for a [Complex64VectorValue].
     */
    class Complex64Vector(query: Complex64VectorValue) : EuclideanKernel<Complex64VectorValue>(query) {
        override fun invoke(vector: Complex64VectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2)
            }
            return DoubleValue(kotlin.math.sqrt(sum))
        }
    }

    /**
     * [EuclideanKernel] for a [Complex32VectorValue].
     */
    class Complex32Vector(query: Complex32VectorValue) : EuclideanKernel<Complex32VectorValue>(query) {
        override fun invoke(vector: Complex32VectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2)
            }
            return DoubleValue(kotlin.math.sqrt(sum))
        }
    }

    /**
     * [EuclideanKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue) : EuclideanKernel<DoubleVectorValue>(query) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2)
            }
            return DoubleValue(kotlin.math.sqrt(sum))
        }
    }

    /**
     * [EuclideanKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue) : EuclideanKernel<FloatVectorValue>(query) {
        override fun invoke(vector: FloatVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2)
            }
            return DoubleValue(kotlin.math.sqrt(sum))
        }
    }

    /**
     * [EuclideanKernel] for a [LongVectorValue].
     */
    class LongVector(query: LongVectorValue) : EuclideanKernel<LongVectorValue>(query) {
        override fun invoke(vector: LongVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).toDouble().pow(2)
            }
            return DoubleValue(kotlin.math.sqrt(sum))
        }
    }

    /**
     * [EuclideanKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue) : EuclideanKernel<IntVectorValue>(query) {
        override fun invoke(vector: IntVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).toDouble().pow(2)
            }
            return DoubleValue(kotlin.math.sqrt(sum))
        }
    }
}