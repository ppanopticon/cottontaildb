package org.vitrivr.cottontail.functions.math.distance

import org.vitrivr.cottontail.functions.FunctionRegistry
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.functions.math.distance.binary.*
import org.vitrivr.cottontail.functions.math.distance.binary.simd.ManhattanDistance
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A kernel function used for distance calculations between a query [VectorValue] and other vector values.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface VectorDistance<T : VectorValue<*>>: Function.Dynamic<DoubleValue> {

    companion object {
        /**
         * Registers default [VectorDistance] functions.
         *
         * @param registry [FunctionRegistry] to register with.
         */
        fun register(registry: FunctionRegistry) {
            registry.register(ManhattanDistance.Generator)
            registry.register(EuclideanDistance.Generator)
            registry.register(SquaredEuclideanDistance.Generator)
            registry.register(HammingDistance.Generator)
            registry.register(HaversineDistance.Generator)
            registry.register(CosineDistance.Generator)
            registry.register(ChisquaredDistance.Generator)
            registry.register(InnerProductDistance.Generator)
        }
    }

    /** The name of this [VectorDistance] [Function]. */
    val name: String

    /** The query [VectorValue]. */
    var query: T

    /** The [Type] accepted by this [VectorDistance] [Function]. Must be a vector value type. */
    val type: Type<out T>

    /** The [Signature.Closed] of this [VectorDistance] [Function]. */
    override val signature: Signature.Closed<out DoubleValue>
        get() = Signature.Closed(this.name, Type.Double, arrayOf(this.type))

    /** The dimensionality of this [VectorDistance]. */
    val d: Int
        get() = this.query.logicalSize

    /** For the sake of optimization, [VectorDistance]s are not stateless! */
    override val stateless: Boolean
        get() = false

    /**
     * Replaces the query in this [VectorDistance] by the given [VectorValue].
     *
     * @param query New query [VectorValue]
     */
    fun apply(query: VectorValue<*>) {
        this.query = (query as T)
    }

    /**
     * Creates a copy of this [VectorDistance]. Can be used to create different shapes by choosing a new value for [d].
     *
     * @param d The new dimensionality of the [VectorDistance].
     * @return New [VectorDistance]
     */
    fun copy(d: Int = this.d): VectorDistance<T>

    /**
     * A special type of [VectorDistance] is the [MinkowskiDistance], e.g., the L1, L2 or Lp distance.
     */
    interface MinkowskiDistance<T : VectorValue<*>> : VectorDistance<T> {
        val p: Int
    }
}