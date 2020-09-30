package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

object ChisquaredDistance : DistanceKernel {
    override val cost = 4.25f

    override fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue = (((a - b).pow(2)) / (b + a)).sum().asDouble()

    override fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue = ((((a - b).pow(2)) / (a + b)) * weights).sum().asDouble()
}