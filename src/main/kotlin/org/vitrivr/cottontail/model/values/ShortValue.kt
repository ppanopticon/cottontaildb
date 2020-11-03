package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.RealValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.nextShort
import java.util.*

/**
 * This is an abstraction over a [Short].
 *
 * @author Ralph Gasser
 * @version 1.3.1
 */
inline class ShortValue(override val value: Short): RealValue<Short> {
    companion object {
        val ZERO = ShortValue(0.toShort())
        val ONE = ShortValue(1.toShort())

        /**
         * Generates a random [ShortValue].
         *
         * @param rnd A [SplittableRandom] to generate the random numbers.
         * @return Random [ShortValue]
         */
        fun random(rnd: SplittableRandom = Value.RANDOM) = ShortValue(rnd.nextShort())
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [ShortValue]
     */
    constructor(number: Number) : this(number.toShort())

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [ShortValue]
     */
    constructor(number: NumericValue<*>) : this(number.value.toShort())

    override val logicalSize: Int
        get() = -1

    override val real: RealValue<Short>
        get() = this

    override val imaginary: RealValue<Short>
        get() = ZERO

    override fun compareTo(other: Value): Int = when (other) {
        is ByteValue -> this.value.compareTo(other.value)
        is ShortValue -> this.value.compareTo(other.value)
        is IntValue -> this.value.compareTo(other.value)
        is LongValue -> this.value.compareTo(other.value)
        is DoubleValue -> this.value.compareTo(other.value)
        is FloatValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("ShortValues can only be compared to other numeric values.")
    }

    override fun compareTo(other: Number): Int = when (other) {
        is Byte -> this.value.compareTo(other)
        is Short -> this.value.compareTo(other)
        is Int -> this.value.compareTo(other)
        is Long -> this.value.compareTo(other)
        is Double -> this.value.compareTo(other)
        is Float -> this.value.compareTo(other)
        else -> throw IllegalArgumentException("ShortValues can only be compared to other numeric values.")
    }

    override fun asDouble(): DoubleValue = DoubleValue(this.value.toDouble())
    override fun asFloat(): FloatValue = FloatValue(this.value.toFloat())
    override fun asInt(): IntValue = IntValue(this.value.toInt())
    override fun asLong(): LongValue = LongValue(this.value.toLong())
    override fun asShort(): ShortValue = this
    override fun asByte(): ByteValue = ByteValue(this.value.toByte())
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): ShortValue = ShortValue((-this.value).toShort())

    override fun plus(other: NumericValue<*>) = ShortValue((this.value + other.value.toShort()).toShort())
    override fun minus(other: NumericValue<*>) = ShortValue((this.value - other.value.toShort()).toShort())
    override fun times(other: NumericValue<*>) = ShortValue((this.value * other.value.toShort()).toShort())
    override fun div(other: NumericValue<*>) = ShortValue((this.value / other.value.toShort()).toShort())

    override fun abs() = ShortValue(kotlin.math.abs(this.value.toInt()))

    override fun pow(x: Int) = this.asDouble().pow(x)
    override fun pow(x: Double) = this.asDouble().pow(x)
    override fun sqrt() = this.asDouble().sqrt()
    override fun exp() = this.asDouble().exp()
    override fun ln() = this.asDouble().ln()

    override fun cos() = this.asDouble().cos()
    override fun sin() = this.asDouble().sin()
    override fun tan() = this.asDouble().tan()
    override fun atan() = this.asDouble().atan()

    override fun compareTo(other: NumericValue<Short>): Int = this.value.compareTo(other.value)
}