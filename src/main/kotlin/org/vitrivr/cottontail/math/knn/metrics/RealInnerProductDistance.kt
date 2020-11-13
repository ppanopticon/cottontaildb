package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

object RealInnerProductDistance : DistanceKernel {
    /**
     * Estimates of the cost incurred by applying this [RealInnerProductDistance] to a [VectorValue] of size [d].
     *
     * @param d The dimension to calculate the cost for.
     * @return The estimated cost.
     */
    override fun costForDimension(d: Int): Float = 2.25f * Cost.COST_FLOP // todo: assess actual cost

    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue = DoubleValue.ONE - (a dot b).real.asDouble()
    // todo: use dotReal for complex vectors if we can make it faster than calculating the full and taking only the real part

    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue {
        TODO("Not yet implemented")
    }
}