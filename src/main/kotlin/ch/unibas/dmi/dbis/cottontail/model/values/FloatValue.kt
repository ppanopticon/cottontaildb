package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.*
import kotlin.math.pow

inline class FloatValue(override val value: Float): RealValue<Float> {

    companion object {
        val ZERO = FloatValue(0.0f)
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

    override fun compareTo(other: Value): Int = when (other) {
        is ByteValue -> this.value.compareTo(other.value)
        is ShortValue -> this.value.compareTo(other.value)
        is IntValue -> this.value.compareTo(other.value)
        is LongValue -> this.value.compareTo(other.value)
        is DoubleValue -> this.value.compareTo(other.value)
        is FloatValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("FloatValues can only be compared to other numeric values.")
    }

    override fun compareTo(other: Number): Int = when (other) {
        is Byte -> this.value.compareTo(other)
        is Short -> this.value.compareTo(other)
        is Int -> this.value.compareTo(other)
        is Long -> this.value.compareTo(other)
        is Double -> this.value.compareTo(other)
        is Float -> this.value.compareTo(other)
        else -> throw IllegalArgumentException("FloatValues can only be compared to other numeric values.")
    }

    override fun asDouble(): DoubleValue = DoubleValue(this.value.toDouble())
    override fun asFloat(): FloatValue = this
    override fun asInt(): IntValue = IntValue(this.value.toInt())
    override fun asLong(): LongValue = LongValue(this.value.toLong())
    override fun asShort(): ShortValue = ShortValue(this.value.toShort())
    override fun asByte(): ByteValue = ByteValue(this.value.toByte())
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): FloatValue = FloatValue(-this.value)

    override fun plus(other: NumericValue<*>) = FloatValue(this.value + other.value.toFloat())
    override fun minus(other: NumericValue<*>) = FloatValue(this.value - other.value.toFloat())
    override fun times(other: NumericValue<*>) = FloatValue(this.value * other.value.toFloat())
    override fun div(other: NumericValue<*>) = FloatValue(this.value / other.value.toFloat())

    override fun pow(x: Double) = DoubleValue(this.value.pow(x.toFloat()))
    override fun pow(x: Int) = DoubleValue(this.value.pow(x))
    override fun sqrt() = DoubleValue(kotlin.math.sqrt(this.value))
    override fun abs() = DoubleValue(kotlin.math.abs(this.value))

    override fun cos() = FloatValue(kotlin.math.cos(this.value))
    override fun sin() = FloatValue(kotlin.math.sin(this.value))
    override fun tan() = FloatValue(kotlin.math.tan(this.value))
    override fun atan()= FloatValue(kotlin.math.atan(this.value))
}