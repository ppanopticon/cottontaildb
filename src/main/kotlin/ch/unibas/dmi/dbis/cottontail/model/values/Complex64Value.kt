package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.ComplexValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.NumericValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.RealValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import kotlin.math.pow
import kotlin.math.sign
import kotlin.math.sqrt

inline class Complex64Value(val data: DoubleArray): ComplexValue<Double> {
    companion object {
        val ZERO = Complex64Value(DoubleValue(0.0), DoubleValue(0.0))
        val ONE = Complex64Value(DoubleValue(1.0), DoubleValue(1.0))
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [Complex64Value]
     */
    constructor(number: Number) : this(doubleArrayOf(number.toDouble(), 0.0))

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [Complex64Value]
     */
    constructor(number: NumericValue<*>): this(when (number) {
        is ComplexValue<*> -> doubleArrayOf(number.real.value.toDouble(), number.imaginary.value.toDouble())
        else -> doubleArrayOf(number.value.toDouble(), 0.0)
    })

    /**
     * Recommended constructor for two [RealValue]s.
     *
     * @param real The real part of the [Complex64Value].
     * @param imaginary The imaginary part of the [Complex64Value].
     */
    constructor(real: RealValue<*>, imaginary: RealValue<*>): this(real.value, imaginary.value)

    /**
     * Constructor for two [Number]s
     *
     * @param real The real part of the [Complex64Value].
     * @param imaginary The imaginary part of the [Complex64Value].
     */
    constructor(real: Number, imaginary: Number): this(doubleArrayOf(real.toDouble(), imaginary.toDouble()))

    override val value: Double
        get() = this.modulo().value

    override val real: DoubleValue
        get() = DoubleValue(this.data[0])

    override val imaginary: DoubleValue
        get() = DoubleValue(this.data[1])

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
        else -> throw IllegalArgumentException("Complex64Value can only be compared to other numeric values.")
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
        else -> throw IllegalArgumentException("Complex64Value can only be compared to other numeric values.")
    }

    override fun asComplex32(): Complex32Value = Complex32Value(this.real.asFloat(), this.imaginary.asFloat())
    override fun asComplex64(): Complex64Value = this

    /**
     * Calculates and returns the inverse of this [Complex32Value].
     *
     * @return The inverse [Complex32Value].
     */
    override fun inverse() = Complex64Value((this.real / (this.real  * this.real + this.imaginary * this.imaginary)), -(this.imaginary / (this.real* this.real + this.imaginary * this.imaginary)))

    /**
     * Calculates and returns the modulo of this [Complex32Value].
     *
     * @return The module of this [Complex32Value].
     */
    override fun modulo() = DoubleValue(sqrt((this.real * this.real + this.imaginary * this.imaginary).value))

    override fun unaryMinus(): Complex64Value = Complex64Value(-this.real, -this.imaginary)
    override fun plus(other: NumericValue<*>) = Complex64Value(this.real + other.real, this.imaginary - other.imaginary)
    override fun minus(other: NumericValue<*>) = Complex64Value(this.real - other.real, this.imaginary - other.imaginary)
    override fun times(other: NumericValue<*>) = Complex64Value(this.real * other.real - this.imaginary * other.imaginary, this.real * other.imaginary + this.imaginary * other.real)
    override fun div(other: NumericValue<*>): Complex64Value {
        val div = FloatValue(other.real.value.toFloat().pow(2) + other.imaginary.value.toFloat().compareTo(2))
        return Complex64Value(
                (this.real * other.real + this.imaginary * other.imaginary) / div,
                (this.real * other.imaginary - this.imaginary * other.real) / div
        )
    }

    override fun abs(): Complex64Value = Complex64Value(this.real.abs(), this.imaginary.abs())

    override fun pow(x: Double): Complex64Value {
        val r = this.real.value.pow(x) + this.imaginary.value.pow(x)
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