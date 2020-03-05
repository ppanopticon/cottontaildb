package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.NumericValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.RealValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value

inline class ByteValue(override val value: Byte): RealValue<Byte> {

    companion object {
        val ZERO = ByteValue(0.toByte())
    }

    override val logicalSize: Int
        get() = -1

    override val real: RealValue<Byte>
        get() = this

    override val imaginary: RealValue<Byte>
        get() = ZERO

    override fun compareTo(other: Value): Int = when (other) {
        is ByteValue -> this.value.compareTo(other.value)
        is ShortValue -> this.value.compareTo(other.value)
        is IntValue -> this.value.compareTo(other.value)
        is LongValue -> this.value.compareTo(other.value)
        is DoubleValue -> this.value.compareTo(other.value)
        is FloatValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("ByteValues can only be compared to other numeric values.")
    }

    override fun compareTo(other: Number): Int = when (other) {
        is Byte -> this.value.compareTo(other)
        is Short -> this.value.compareTo(other)
        is Int -> this.value.compareTo(other)
        is Long -> this.value.compareTo(other)
        is Double -> this.value.compareTo(other)
        is Float -> this.value.compareTo(other)
        else -> throw IllegalArgumentException("ByteValues can only be compared to other numeric values.")
    }

    override fun asDouble(): DoubleValue = DoubleValue(this.value.toDouble())
    override fun asFloat(): FloatValue = FloatValue(this.value.toFloat())
    override fun asInt(): IntValue = IntValue(this.value.toInt())
    override fun asLong(): LongValue = LongValue(this.value.toLong())
    override fun asShort(): ShortValue = ShortValue(this.value.toShort())
    override fun asByte(): ByteValue = this
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): ByteValue = ByteValue((-this.value).toByte())

    override fun plus(other: NumericValue<*>) = ByteValue((this.value + other.value.toByte()).toByte())
    override fun minus(other: NumericValue<*>)= ByteValue((this.value - other.value.toByte()).toByte())
    override fun times(other: NumericValue<*>) = ByteValue((this.value * other.value.toByte()).toByte())
    override fun div(other: NumericValue<*>) = ByteValue((this.value / other.value.toByte()).toByte())

    override fun pow(x: Double) = this.asDouble().pow(x)
    override fun pow(x: Int) = this.asDouble().pow(x)
    override fun sqrt() = this.asDouble().sqrt()
    override fun abs() = this.asDouble().abs()

    override fun cos() = ByteValue(kotlin.math.cos(this.value.toDouble()).toByte())
    override fun sin() = ByteValue(kotlin.math.sin(this.value.toDouble()).toByte())
    override fun tan() = ByteValue(kotlin.math.tan(this.value.toDouble()).toByte())
    override fun atan() = ByteValue(kotlin.math.atan(this.value.toDouble()).toByte())
}