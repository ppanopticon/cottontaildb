package org.vitrivr.cottontail.math.knn.kernels.binary.plain

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.basics.MinkowskiKernel
import org.vitrivr.cottontail.math.knn.kernels.KernelNotFoundException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue

import kotlin.math.absoluteValue
import kotlin.math.pow

/**
 * A [ManhattanKernel] implementation to calculate Manhattan or L1 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class ManhattanKernel<T : VectorValue<*>>(final override val query: T) : MinkowskiKernel<T> {
    companion object {
        /**
         * Returns the [ManhattanKernel] implementation for the given [VectorValue].
         *
         * @param query [VectorValue] The [VectorValue] to return the [ManhattanKernel] for.
         * @return [ManhattanKernel]
         * @throws KernelNotFoundException If no supported [ManhattanKernel] could not be found.
         */
        fun kernel(query: VectorValue<*>) = when (query) {
            is Complex64VectorValue -> ManhattanKernel.Complex64Vector(query)
            is Complex32VectorValue -> ManhattanKernel.Complex32Vector(query)
            is DoubleVectorValue -> ManhattanKernel.DoubleVector(query)
            is FloatVectorValue -> ManhattanKernel.FloatVector(query)
            is IntVectorValue -> ManhattanKernel.IntVector(query)
            is LongVectorValue -> ManhattanKernel.LongVector(query)
            else -> throw KernelNotFoundException(ManhattanKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [ManhattanKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (2.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS)
    }

    /** The [p] value for an [ManhattanKernel] instance is always 2. */
    final override val p: Int = 1

    /** The cost of applying this [ManhattanKernel] to a single [VectorValue]. */
    override val cost: Float
        get() = cost(this.d)

    /**
     * [ManhattanKernel] for a [Complex64VectorValue].
     */
    class Complex64Vector(query: Complex64VectorValue) : ManhattanKernel<Complex64VectorValue>(query) {
        override fun invoke(vector: Complex64VectorValue): DoubleValue {
            var sum = 0.0
            for (i in 0 until this.query.data.size / 2) {
                val diffReal = this.query.data[i shl 1] - vector.data[i shl 1]
                val diffImaginary = this.query.data[(i shl 1) + 1] - vector.data[(i shl 1) + 1]
                sum += kotlin.math.sqrt(diffReal.pow(2) + diffImaginary.pow(2))
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ManhattanKernel] for a [Complex32VectorValue].
     */
    class Complex32Vector(query: Complex32VectorValue) : ManhattanKernel<Complex32VectorValue>(query) {
        override fun invoke(vector: Complex32VectorValue): DoubleValue {
            var sum = 0.0
            for (i in 0 until this.query.data.size / 2) {
                val diffReal = this.query.data[i shl 1] - vector.data[i shl 1]
                val diffImaginary = this.query.data[(i shl 1) + 1] - vector.data[(i shl 1) + 1]
                sum += kotlin.math.sqrt(diffReal.pow(2) + diffImaginary.pow(2))
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [EuclideanKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue) : ManhattanKernel<DoubleVectorValue>(query) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [EuclideanKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue) : ManhattanKernel<FloatVectorValue>(query) {
        override fun invoke(vector: FloatVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [EuclideanKernel] for a [LongVectorValue].
     */
    class LongVector(query: LongVectorValue) : ManhattanKernel<LongVectorValue>(query) {
        override fun invoke(vector: LongVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [EuclideanKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue) : ManhattanKernel<IntVectorValue>(query) {
        override fun invoke(vector: IntVectorValue): DoubleValue {
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }
}