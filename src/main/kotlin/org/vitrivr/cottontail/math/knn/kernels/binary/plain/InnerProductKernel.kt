package org.vitrivr.cottontail.math.knn.kernels.binary.plain

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.math.knn.kernels.KernelNotFoundException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A [DistanceKernel] implementation to calculate the inner product distance between [query] and a series of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class InnerProductKernel<T : VectorValue<*>>(final override val query: T) : DistanceKernel<T> {

    companion object {
        /**
         * Returns the [EuclideanKernel] implementation for the given [VectorValue].
         *
         * @param query [VectorValue] The [VectorValue] to return the [EuclideanKernel] for.
         * @return [EuclideanKernel]
         * @throws KernelNotFoundException If no supported [EuclideanKernel] could not be found.
         */
        fun kernel(query: VectorValue<*>) = when (query) {
            is Complex64VectorValue -> Complex64Vector(query)
            is Complex32VectorValue -> Complex32Vector(query)
            is DoubleVectorValue -> DoubleVector(query)
            is FloatVectorValue -> FloatVector(query)
            is IntVectorValue -> IntVector(query)
            is LongVectorValue -> LongVector(query)
            else -> throw KernelNotFoundException(EuclideanKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [EuclideanKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (3.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP
    }

    /** The cost of applying this [EuclideanKernel] to a single [VectorValue]. */
    override val cost: Float
        get() = cost(this.d)

    /**
     * [InnerProductKernel] for a [Complex64VectorValue].
     */
    class Complex64Vector(query: Complex64VectorValue) : InnerProductKernel<Complex64VectorValue>(query) {
        override fun invoke(vector: Complex64VectorValue): DoubleValue {
            var real = 0.0
            var imaginary = 0.0
            for (i in 0 until this.query.logicalSize) {
                val iprime = i shl 1
                real += this.query.data[iprime] * vector.data[iprime] + this.query.data[iprime + 1] * vector.data[iprime + 1]
                imaginary += this.query.data[iprime + 1] * vector.data[iprime] - this.query.data[iprime] * vector.data[iprime + 1]
            }
            return DoubleValue(1.0) - Complex64Value(real, imaginary).abs()
        }
    }

    /**
     * [InnerProductKernel] for a [Complex32VectorValue].
     */
    class Complex32Vector(query: Complex32VectorValue) : InnerProductKernel<Complex32VectorValue>(query) {
        override fun invoke(vector: Complex32VectorValue): DoubleValue {
            var real = 0.0
            var imaginary = 0.0
            for (i in 0 until this.query.logicalSize) {
                val iprime = i shl 1
                real += this.query.data[iprime] * vector.data[iprime] + this.query.data[iprime + 1] * vector.data[iprime + 1]
                imaginary += this.query.data[iprime + 1] * vector.data[iprime] - this.query.data[iprime] * vector.data[iprime + 1]
            }
            return Complex64Value(real, imaginary).abs()
        }
    }

    /**
     * [InnerProductKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue) : InnerProductKernel<DoubleVectorValue>(query) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue {
            var dotp = 0.0
            for (i in this.query.data.indices) {
                dotp += this.query.data[i] * vector.data[i]
            }
            return DoubleValue(dotp)
        }
    }

    /**
     * [InnerProductKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue) : InnerProductKernel<FloatVectorValue>(query) {
        override fun invoke(vector: FloatVectorValue): DoubleValue {
            var dotp = 0.0
            for (i in this.query.data.indices) {
                dotp += this.query.data[i] * vector.data[i]
            }
            return DoubleValue(dotp)
        }
    }

    /**
     * [InnerProductKernel] for a [LongVectorValue].
     */
    class LongVector(query: LongVectorValue) : InnerProductKernel<LongVectorValue>(query) {
        override fun invoke(vector: LongVectorValue): DoubleValue {
            var dotp = 0.0
            for (i in this.query.data.indices) {
                dotp += this.query.data[i] * vector.data[i]
            }
            return DoubleValue(dotp)
        }
    }

    /**
     * [InnerProductKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue) : InnerProductKernel<IntVectorValue>(query) {
        override fun invoke(vector: IntVectorValue): DoubleValue {
            var dotp = 0.0
            for (i in this.query.data.indices) {
                dotp += this.query.data[i] * vector.data[i]
            }
            return DoubleValue(dotp)
        }
    }
}