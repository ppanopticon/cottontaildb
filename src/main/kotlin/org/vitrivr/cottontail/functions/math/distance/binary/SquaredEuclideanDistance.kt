package org.vitrivr.cottontail.functions.math.distance.binary

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.AbstractFunctionGenerator
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.basics.FunctionGenerator
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.VectorDistance
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.pow

/**
 * A [SquaredEuclideanDistance] implementation to calculate the squared Euclidean or L2 distance between a [query] and a series of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class SquaredEuclideanDistance<T : VectorValue<*>> : VectorDistance.MinkowskiDistance<T> {

    /**
     * The [FunctionGenerator] for the [SquaredEuclideanDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        const val FUNCTION_NAME = "squaredeuclidean"

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, Type.Double, arity = 1)

        override fun generateInternal(vararg arguments: Type<*>): Function.Dynamic<DoubleValue> = when (arguments[0]) {
            is Type.Complex64Vector -> Complex64Vector(arguments[0].logicalSize)
            is Type.Complex32Vector -> Complex32Vector(arguments[0].logicalSize)
            is Type.DoubleVector -> DoubleVector(arguments[0].logicalSize)
            is Type.FloatVector -> FloatVector(arguments[0].logicalSize)
            is Type.LongVector -> LongVector(arguments[0].logicalSize)
            is Type.IntVector -> IntVector(arguments[0].logicalSize)
            else -> throw FunctionNotSupportedException(this.signature)
        }
    }

    /** Name of this [SquaredEuclideanDistance]. */
    override val name: String = Generator.FUNCTION_NAME

    /** The [p] value for an [SquaredEuclideanDistance] instance is always 2. */
    final override val p: Int = 2

    /** The cost of applying this [SquaredEuclideanDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = d * (3.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP + Cost.COST_MEMORY_ACCESS

    /**
     * [SquaredEuclideanDistance] for a [Complex64VectorValue].
     */
    class Complex64Vector(size: Int) : SquaredEuclideanDistance<Complex64VectorValue>() {
        override val type = Type.Complex64Vector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as Complex64VectorValue
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [Complex32VectorValue].
     */
    class Complex32Vector(size: Int) : SquaredEuclideanDistance<Complex32VectorValue>() {
        override val type = Type.Complex32Vector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as Complex32VectorValue
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : SquaredEuclideanDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as DoubleVectorValue
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : SquaredEuclideanDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as FloatVectorValue
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : SquaredEuclideanDistance<LongVectorValue>() {
        override val type = Type.LongVector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as LongVectorValue
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).toDouble().pow(2)
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : SquaredEuclideanDistance<IntVectorValue>() {
        override val type = Type.IntVector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as IntVectorValue
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += (this.query.data[i] - vector.data[i]).toDouble().pow(2)
            }
            return DoubleValue(sum)
        }
    }
}