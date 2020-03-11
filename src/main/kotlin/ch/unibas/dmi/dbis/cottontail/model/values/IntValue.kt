package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.NumericValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.RealValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value

inline class IntValue(override val value: Int): RealValue<Int> {

    companion object {
        val ZERO = IntValue(0)
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [IntValue]
     */
    constructor(number: Number) : this(number.toInt())

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [IntValue]
     */
    constructor(number: NumericValue<*>) : this(number.value.toInt())

    override val logicalSize: Int
        get() = -1

    override val real: RealValue<Int>
        get() = this

    override val imaginary: RealValue<Int>
        get() = ZERO

    override fun compareTo(other: Value): Int = when (other) {
        is ByteValue -> this.value.compareTo(other.value)
        is ShortValue -> this.value.compareTo(other.value)
        is IntValue -> this.value.compareTo(other.value)
        is LongValue -> this.value.compareTo(other.value)
        is DoubleValue -> this.value.compareTo(other.value)
        is FloatValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("IntValues can only be compared to other numeric values.")
    }

    override fun compareTo(other: Number): Int = when (other) {
        is Byte -> this.value.compareTo(other)
        is Short -> this.value.compareTo(other)
        is Int -> this.value.compareTo(other)
        is Long -> this.value.compareTo(other)
        is Double -> this.value.compareTo(other)
        is Float -> this.value.compareTo(other)
        else -> throw IllegalArgumentException("IntValues can only be compared to other numeric values.")
    }

    override fun asDouble(): DoubleValue = DoubleValue(this.value.toDouble())
    override fun asFloat(): FloatValue = FloatValue(this.value.toFloat())
    override fun asInt(): IntValue = this
    override fun asLong(): LongValue = LongValue(this.value.toLong())
    override fun asShort(): ShortValue = ShortValue(this.value.toShort())
    override fun asByte(): ByteValue = ByteValue(this.value.toByte())
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): IntValue = IntValue(-this.value)

    override fun plus(other: NumericValue<*>) =IntValue(this.value + other.value.toInt())
    override fun minus(other: NumericValue<*>) = IntValue(this.value - other.value.toInt())
    override fun times(other: NumericValue<*>) = IntValue(this.value * other.value.toInt())
    override fun div(other: NumericValue<*>) = IntValue(this.value / other.value.toInt())

    override fun pow(x: Double) = this.asDouble().pow(x)
    override fun pow(x: Int) = this.asDouble().pow(x)
    override fun sqrt() = this.asDouble().sqrt()
    override fun abs() = this.asDouble().abs()

    override fun cos() = IntValue(kotlin.math.cos(this.value.toDouble()).toInt())
    override fun sin() = IntValue(kotlin.math.sin(this.value.toDouble()).toInt())
    override fun tan() = IntValue(kotlin.math.tan(this.value.toDouble()).toInt())
    override fun atan() = IntValue(kotlin.math.atan(this.value.toDouble()).toInt())
}
