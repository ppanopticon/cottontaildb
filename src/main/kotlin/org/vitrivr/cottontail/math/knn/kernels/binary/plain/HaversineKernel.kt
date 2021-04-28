package org.vitrivr.cottontail.math.knn.kernels.binary.plain

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.math.knn.kernels.KernelNotFoundException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.atan2
import kotlin.math.cos
import kotlin.math.sin
import kotlin.math.sqrt

/**
 * A [DistanceKernel] implementation to calculate the Haversine distance between two 2D points.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.0
 */
sealed class HaversineKernel<T : VectorValue<*>>(final override val query: T) : DistanceKernel<T> {

    companion object {
        const val RADIUS_EARTH = 6371000.0

        /**
         * Returns the [HaversineKernel] implementation for the given [VectorValue].
         *
         * @param query [VectorValue] The [VectorValue] to return the [HaversineKernel] for.
         * @return [HaversineKernel]
         * @throws KernelNotFoundException If no supported [HaversineKernel] implementation could not be found.
         */
        fun kernel(query: VectorValue<*>) = when (query) {
            is DoubleVectorValue -> HaversineKernel.DoubleVector(query)
            is FloatVectorValue -> HaversineKernel.FloatVector(query)
            is IntVectorValue -> HaversineKernel.IntVector(query)
            is LongVectorValue -> HaversineKernel.LongVector(query)
            else -> throw KernelNotFoundException(HaversineKernel::class.java.simpleName, query)
        }

        /**
         * Calculates the cost of applying a [HammingKernel] of dimension [d] to a vector.
         *
         * @param d The dimension used for cost calculation.
         * @return Estimated cost.
         */
        fun cost(d: Int) = d * (Cost.COST_FLOP + Cost.COST_MEMORY_ACCESS)
    }

    /** The cost of applying this [EuclideanKernel] to a single [VectorValue]. */
    override val cost: Float
        get() = this.d * 2.0f * Cost.COST_MEMORY_ACCESS + 27.0f * Cost.COST_FLOP + 20.0f * Cost.COST_MEMORY_ACCESS

    init {
        require(this.query.logicalSize == 2) { "Haversine distance can only be calculated for 2D vectors." }
    }

    /**
     * Calculates the haversine distance between two points a, b.
     *
     * @param aLat The latitude of a.
     * @param aLon The longitude of a.
     * @param bLat The latitude of b.
     * @param bLon The longitude of b.
     */
    protected fun haversine(aLat: Double, aLon: Double, bLat: Double, bLon: Double): Double {
        val phi1 = StrictMath.toRadians(aLat)
        val phi2 = StrictMath.toRadians(bLat)
        val deltaPhi = StrictMath.toRadians(bLat - aLat)
        val deltaLambda = StrictMath.toRadians(bLon - aLon)
        val c = sin(deltaPhi / 2.0) * sin(deltaPhi / 2.0) + cos(phi1) * cos(phi2) * sin(deltaLambda / 2.0) * sin(deltaLambda / 2.0)
        val d = 2.0 * atan2(sqrt(c), sqrt(1 - c))
        return RADIUS_EARTH * d
    }

    /**
     * [EuclideanKernel] for a [DoubleVectorValue].
     */
    class DoubleVector(query: DoubleVectorValue) : HaversineKernel<DoubleVectorValue>(query) {
        override fun invoke(vector: DoubleVectorValue): DoubleValue = DoubleValue(haversine(this.query.data[0], this.query.data[1], vector.data[0], vector.data[1]))
    }

    /**
     * [EuclideanKernel] for a [FloatVectorValue].
     */
    class FloatVector(query: FloatVectorValue) : HaversineKernel<FloatVectorValue>(query) {
        override fun invoke(vector: FloatVectorValue): DoubleValue = DoubleValue(haversine(this.query.data[0].toDouble(), this.query.data[1].toDouble(), vector.data[0].toDouble(), vector.data[1].toDouble()))
    }

    /**
     * [EuclideanKernel] for a [LongVectorValue].
     */
    class LongVector(d: LongVectorValue) : HaversineKernel<LongVectorValue>(d) {
        override fun invoke(vector: LongVectorValue): DoubleValue = DoubleValue(haversine(this.query.data[0].toDouble(), this.query.data[1].toDouble(), vector.data[0].toDouble(), vector.data[1].toDouble()))
    }

    /**
     * [EuclideanKernel] for a [IntVectorValue].
     */
    class IntVector(query: IntVectorValue) : HaversineKernel<IntVectorValue>(query) {
        override fun invoke(vector: IntVectorValue): DoubleValue = DoubleValue(haversine(this.query.data[0].toDouble(), this.query.data[1].toDouble(), vector.data[0].toDouble(), vector.data[1].toDouble()))
    }
}