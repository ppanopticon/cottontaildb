package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

object RealInnerProductDistance : DistanceKernel {
 override val cost = 3.0f
 // todo: assess actual cost

 override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue = DoubleValue.ONE - (a dot b).real.asDouble()
 // todo: use dotReal for complex vectors if we can make it faster than calculating the full and taking only the real part

 override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue {
  TODO("Not yet implemented")
 }
}