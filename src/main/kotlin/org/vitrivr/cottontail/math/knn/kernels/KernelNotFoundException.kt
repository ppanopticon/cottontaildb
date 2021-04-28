package org.vitrivr.cottontail.math.knn.kernels

import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class KernelNotFoundException(kernelName: String, vector: VectorValue<*>) : Throwable("$kernelName does not support vectors of type ${vector.type}")