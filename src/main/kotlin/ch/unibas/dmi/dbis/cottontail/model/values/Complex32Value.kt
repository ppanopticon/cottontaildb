package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.ComplexValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.NumericValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.RealValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value

import kotlin.math.pow
import kotlin.math.sign
import kotlin.math.sqrt

inline class Complex32Value(val data: FloatArray): ComplexValue<Float> {


    companion object {
        val ZERO = Complex32Value(FloatValue(0.0f), FloatValue(0.0f))
        val ONE = Complex32Value(FloatValue(1.0f), FloatValue(1.0f))
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [DoubleValue]
     */
    constructor(number: Number) : this(floatArrayOf(number.toFloat(), 0.0f))

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [DoubleValue]
     */
    constructor(number: NumericValue<*>): this(when (number) {
        is ComplexValue<*> -> floatArrayOf(number.real.value.toFloat(), number.imaginary.value.toFloat())
        else -> floatArrayOf(number.value.toFloat(), 0.0f)
    })

    /**
     * Constructor for two [RealValue]s
     *
     * @param real The real part of the [Complex32Value].
     * @param imaginary The imaginary part of the [Complex32Value].
     */
    constructor(real: RealValue<*>, imaginary: RealValue<*>): this(real.value, imaginary.value)

    /**
     * Constructor for two [Number]s
     *
     * @param real The real part of the [Complex32Value].
     * @param imaginary The imaginary part of the [Complex32Value].
     */
    constructor(real: Number, imaginary: Number): this(floatArrayOf(real.toFloat(), imaginary.toFloat()))

    override val value: Float
        get() = this.modulo().value

    override val real: FloatValue
        get() = FloatValue(this.data[0])

    override val imaginary: FloatValue
        get() = FloatValue(this.data[1])

    override val logicalSize: Int
        get() = -1

    /**
     * Comparison to other [Value]s. When compared to a [RealValue], then only the real part of the [Complex32Value] is considered.
     */
    override fun compareTo(other: Value): Int = when (other) {
        is Complex32Value -> (modulo() - other.modulo()).value.toInt().sign
        is Complex64Value -> (modulo() - other.modulo()).value.toInt().sign
        is ByteValue -> this.real.compareTo(other)
        is ShortValue -> this.real.compareTo(other)
        is IntValue -> this.real.compareTo(other)
        is LongValue -> this.real.compareTo(other)
        is DoubleValue -> this.real.compareTo(other)
        is FloatVectorValue -> this.real.compareTo(other)
        else -> throw IllegalArgumentException("Complex32Values can only be compared to other numeric values.")
    }

    /**
     * Comparison to [Number]s. When compared to a [Number], then only the real part of the [Complex32Value] is considered.
     */
    override fun compareTo(other: Number): Int =  when (other) {
        is Byte -> this.real.compareTo(other)
        is Short -> this.real.compareTo(other)
        is Int -> this.real.compareTo(other)
        is Long -> this.real.compareTo(other)
        is Double -> this.real.compareTo(other)
        is Float -> this.real.compareTo(other)
        else -> throw IllegalArgumentException("Complex32Values can only be compared to other numeric values.")
    }

    override fun asComplex32(): Complex32Value = this
    override fun asComplex64(): Complex64Value = Complex64Value(this.real.asDouble(), this.imaginary.asDouble())

    /**
     * Calculates and returns the inverse of this [Complex32Value].
     *
     * @return The inverse [Complex32Value].
     */
    override fun inverse(): Complex32Value = Complex32Value(floatArrayOf((this.real / (this.real  * this.real + this.imaginary * this.imaginary)).value, -(this.imaginary / (this.real* this.real + this.imaginary * this.imaginary)).value))

    /**
     * Calculates and returns the modulo of this [Complex32Value].
     *
     * @return The module of this [Complex32Value].
     */
    override fun modulo() = FloatValue(sqrt((this.real * this.real + this.imaginary * this.imaginary).value))

    override fun unaryMinus() = Complex32Value(-this.real, -this.imaginary)
    override fun plus(other: NumericValue<*>) = Complex32Value(this.real + other.real, this.imaginary + other.imaginary)
    override fun minus(other: NumericValue<*>) = Complex32Value(this.real - other.real, this.imaginary - other.imaginary)
    override fun times(other: NumericValue<*>) = Complex32Value(this.real * other.real - this.imaginary * other.imaginary, this.real * other.imaginary + this.imaginary * other.real)
    override fun div(other: NumericValue<*>): Complex32Value {
        val div = FloatValue(other.real.value.toFloat().pow(2) + other.imaginary.value.toFloat().compareTo(2))
        return Complex32Value(
            (this.real * other.real + this.imaginary * other.imaginary) / div,
            (this.real * other.imaginary - this.imaginary * other.real) / div
        )
    }
    override fun abs() = Complex64Value(this.real.abs(), this.imaginary.abs())

    override fun pow(x: Double): Complex64Value {
        val r = this.real.value.pow(x.toFloat()) + this.imaginary.value.pow(x.toFloat())
        val theta =  this.imaginary.value / this.real.value
        return Complex64Value(r * kotlin.math.cos(x*theta), r * kotlin.math.sin(x*theta))
    }

    override fun pow(x: Int): Complex64Value {
        val r = this.real.value.pow(x) + this.imaginary.value.pow(x)
        val theta =  this.imaginary.value / this.real.value
        return Complex64Value(r * kotlin.math.cos(x*theta), r * kotlin.math.sin(x*theta))
    }

    override fun sqrt(): Complex64Value = pow(0.5)
}