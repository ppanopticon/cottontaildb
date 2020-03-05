package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.NumericValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.RealValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import kotlin.math.pow

inline class DoubleValue(override val value: Double): RealValue<Double> {

    companion object {
        val ZERO = DoubleValue(0.0)
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [DoubleValue]
     */
    constructor(number: Number) : this(number.toDouble())

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [DoubleValue]
     */
    constructor(number: NumericValue<*>) : this(number.value.toDouble())

    override val logicalSize: Int
        get() = -1

    override val real: RealValue<Double>
        get() = this

    override val imaginary: RealValue<Double>
        get() = ZERO

    override fun compareTo(other: Value): Int = when (other) {
        is ByteValue -> this.value.compareTo(other.value)
        is ShortValue -> this.value.compareTo(other.value)
        is IntValue -> this.value.compareTo(other.value)
        is LongValue -> this.value.compareTo(other.value)
        is DoubleValue -> this.value.compareTo(other.value)
        is FloatValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("DoubleValues can only be compared to other numeric values.")
    }

    override fun compareTo(other: Number): Int = when (other) {
        is Byte -> this.value.compareTo(other)
        is Short -> this.value.compareTo(other)
        is Int -> this.value.compareTo(other)
        is Long -> this.value.compareTo(other)
        is Double -> this.value.compareTo(other)
        is Float -> this.value.compareTo(other)
        else -> throw IllegalArgumentException("DoubleValues can only be compared to other numeric values.")
    }

    override fun asDouble(): DoubleValue = this
    override fun asFloat(): FloatValue = FloatValue(this.value.toFloat())
    override fun asInt(): IntValue = IntValue(this.value.toInt())
    override fun asLong(): LongValue = LongValue(this.value.toLong())
    override fun asShort(): ShortValue = ShortValue(this.value.toShort())
    override fun asByte(): ByteValue = ByteValue(this.value.toByte())
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): DoubleValue = DoubleValue(-this.value)

    override fun plus(other: NumericValue<*>) = DoubleValue(this.value + other.value.toDouble())
    override fun minus(other: NumericValue<*>) = DoubleValue(this.value - other.value.toDouble())
    override fun times(other: NumericValue<*>) = DoubleValue(this.value * other.value.toDouble())
    override fun div(other: NumericValue<*>) = DoubleValue(this.value / other.value.toDouble())

    override fun pow(x: Double) = DoubleValue(this.value.pow(x))
    override fun pow(x: Int) = DoubleValue(this.value.pow(x))
    override fun sqrt() = DoubleValue(kotlin.math.sqrt(this.value))
    override fun abs() = DoubleValue(kotlin.math.abs(this.value))

    override fun cos() = DoubleValue(kotlin.math.cos(this.value))
    override fun sin() = DoubleValue(kotlin.math.sin(this.value))
    override fun tan() = DoubleValue(kotlin.math.tan(this.value))
    override fun atan() = DoubleValue(kotlin.math.atan(this.value))
}