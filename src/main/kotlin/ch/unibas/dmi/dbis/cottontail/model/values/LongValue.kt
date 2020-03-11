package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.NumericValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.RealValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value

inline class LongValue(override val value: Long): RealValue<Long> {

    companion object {
        val ZERO = LongValue(0L)
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [LongValue]
     */
    constructor(number: Number) : this(number.toLong())

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [LongValue]
     */
    constructor(number: NumericValue<*>) : this(number.value.toLong())

    override val logicalSize: Int
        get() = -1

    override val real: RealValue<Long>
        get() = this

    override val imaginary: RealValue<Long>
        get() = ZERO

    override fun compareTo(other: Value): Int = when (other) {
        is ByteValue -> this.value.compareTo(other.value)
        is ShortValue -> this.value.compareTo(other.value)
        is IntValue -> this.value.compareTo(other.value)
        is LongValue -> this.value.compareTo(other.value)
        is DoubleValue -> this.value.compareTo(other.value)
        is FloatValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("LongValues can only be compared to other numeric values.")
    }

    override fun compareTo(other: Number): Int = when (other) {
        is Byte -> this.value.compareTo(other)
        is Short -> this.value.compareTo(other)
        is Int -> this.value.compareTo(other)
        is Long -> this.value.compareTo(other)
        is Double -> this.value.compareTo(other)
        is Float -> this.value.compareTo(other)
        else -> throw IllegalArgumentException("LongValues can only be compared to other numeric values.")
    }

    override fun asDouble(): DoubleValue = DoubleValue(this.value.toDouble())
    override fun asFloat(): FloatValue = FloatValue(this.value.toFloat())
    override fun asInt(): IntValue = IntValue(this.value.toInt())
    override fun asLong(): LongValue = this
    override fun asShort(): ShortValue = ShortValue(this.value.toShort())
    override fun asByte(): ByteValue = ByteValue(this.value.toByte())
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): LongValue = LongValue(-this.value)

    override fun plus(other: NumericValue<*>) = LongValue(this.value + other.value.toLong())
    override fun minus(other: NumericValue<*>) = LongValue(this.value - other.value.toLong())
    override fun times(other: NumericValue<*>) = LongValue(this.value * other.value.toLong())
    override fun div(other: NumericValue<*>) = LongValue(this.value / other.value.toLong())

    override fun pow(x: Double) = this.asDouble().pow(x)
    override fun pow(x: Int) = this.asDouble().pow(x)
    override fun sqrt() = this.asDouble().sqrt()
    override fun abs() = this.asDouble().abs()

    override fun cos() = LongValue(kotlin.math.cos(this.value.toDouble()).toLong())
    override fun sin() = LongValue(kotlin.math.sin(this.value.toDouble()).toLong())
    override fun tan() = LongValue(kotlin.math.tan(this.value.toDouble()).toLong())
    override fun atan() = LongValue(kotlin.math.atan(this.value.toDouble()).toLong())

}