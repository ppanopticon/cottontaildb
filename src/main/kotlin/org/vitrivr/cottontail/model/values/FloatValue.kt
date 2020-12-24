package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.RealValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.nextFloat
import java.util.*
import kotlin.math.pow

/**
 * This is an abstraction over a [Float].
 *
 * @author Ralph Gasser
 * @version 1.3.1
 */
inline class FloatValue(override val value: Float): RealValue<Float> {

    companion object {
        val ZERO = FloatValue(0.0f)
        val ONE = FloatValue(1.0f)
        val NaN = Complex32Value(Float.NaN)
        val INF = Complex32Value(Float.POSITIVE_INFINITY)

        /**
         * Generates a random [FloatValue].
         *
         * @param rnd A [SplittableRandom] to generate the random numbers.
         * @return Random [FloatValue]
         */
        fun random(rnd: SplittableRandom = Value.RANDOM) = FloatValue(rnd.nextFloat())
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [FloatValue]
     */
    constructor(number: Number) : this(number.toFloat())

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [FloatValue]
     */
    constructor(number: NumericValue<*>) : this(number.value.toFloat())

    override val logicalSize: Int
        get() = -1

    override val real: RealValue<Float>
        get() = this

    override val imaginary: RealValue<Float>
        get() = ZERO

    /**
     * Compares this [FloatValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [FloatValue] can only be compared to other [NumericValue]s.
     *
     * @param other Value to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    override fun compareTo(other: Value): Int = when (other) {
        is ByteValue -> this.value.compareTo(other.value)
        is ShortValue -> this.value.compareTo(other.value)
        is IntValue -> this.value.compareTo(other.value)
        is LongValue -> this.value.compareTo(other.value)
        is DoubleValue -> this.value.compareTo(other.value)
        is FloatValue -> this.value.compareTo(other.value)
        is Complex32Value -> this.value.compareTo(other.data[0])
        is Complex64Value -> this.value.compareTo(other.data[0])
        else -> throw IllegalArgumentException("LongValues can only be compared to other numeric values.")
    }

    /**
     * Checks for equality between this [FloatValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [FloatValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is FloatValue) && (other.value == this.value)

    override fun asDouble(): DoubleValue = DoubleValue(this.value.toDouble())
    override fun asFloat(): FloatValue = this
    override fun asInt(): IntValue = IntValue(this.value.toInt())
    override fun asLong(): LongValue = LongValue(this.value.toLong())
    override fun asShort(): ShortValue = ShortValue(this.value.toInt().toShort())
    override fun asByte(): ByteValue = ByteValue(this.value.toInt().toByte())
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): FloatValue = FloatValue(-this.value)

    override fun plus(other: NumericValue<*>) = FloatValue(this.value + other.value.toFloat())
    override fun minus(other: NumericValue<*>) = FloatValue(this.value - other.value.toFloat())
    override fun times(other: NumericValue<*>) = FloatValue(this.value * other.value.toFloat())
    override fun div(other: NumericValue<*>) = FloatValue(this.value / other.value.toFloat())

    override fun abs() = FloatValue(kotlin.math.abs(this.value))

    override fun pow(x: Double) = DoubleValue(this.value.pow(x.toFloat()))
    override fun pow(x: Int) = FloatValue(this.value.pow(x))
    override fun sqrt() = FloatValue(kotlin.math.sqrt(this.value))
    override fun exp() = DoubleValue(kotlin.math.exp(this.value))
    override fun ln() = DoubleValue(kotlin.math.ln(this.value))

    override fun cos() = DoubleValue(kotlin.math.cos(this.value))
    override fun sin() = DoubleValue(kotlin.math.sin(this.value))
    override fun tan() = DoubleValue(kotlin.math.tan(this.value))
    override fun atan() = DoubleValue(kotlin.math.atan(this.value))
}