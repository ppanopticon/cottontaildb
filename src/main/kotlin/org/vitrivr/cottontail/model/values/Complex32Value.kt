package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.ComplexValue
import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.RealValue
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*

/**
 * Represents a complex number backed by single-precision (32bit) [Float]s
 *
 * @version 1.3.2
 * @author Ralph Gasser
 */
inline class Complex32Value(val data: FloatArray): ComplexValue<Float> {

    companion object {
        val I = Complex32Value(floatArrayOf(0.0f, 1.0f))
        val ZERO = Complex32Value(floatArrayOf(0.0f, 0.0f))
        val ONE = Complex32Value(floatArrayOf(1.0f, 0.0f))
        val NaN = Complex32Value(floatArrayOf(Float.NaN, Float.NaN))
        val INF = Complex32Value(floatArrayOf(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY))

        /**
         * Generates a [Complex32VectorValue] initialized with random numbers.
         *
         * @param rnd A [SplittableRandom] to generate the random numbers.
         * @return Random [Complex32Value]
         */
        fun random(rnd: SplittableRandom = Value.RANDOM) = Complex32Value(FloatArray(2) {
            rnd.nextDouble().toFloat()
        })
    }

    /**
     * Constructor for one [Float]
     *
     * @param number The real part of the [Complex32Value].
     */
    constructor(real: Float) : this(floatArrayOf(real, 0.0f))

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
    constructor(real: RealValue<*>, imaginary: RealValue<*>) : this(real.value, imaginary.value)

    /**
     * Constructor for two [Float]s
     *
     * @param real The real part of the [Complex32Value].
     * @param imaginary The imaginary part of the [Complex32Value].
     */
    constructor(real: Float, imaginary: Float) : this(floatArrayOf(real, imaginary))

    /**
     * Constructor for two [Number]s
     *
     * @param real The real part of the [Complex32Value].
     * @param imaginary The imaginary part of the [Complex32Value].
     */
    constructor(real: Number, imaginary: Number) : this(floatArrayOf(real.toFloat(), imaginary.toFloat()))

    override val value: Float
        get() = this.data[0]

    override val real: FloatValue
        get() = FloatValue(this.data[0])

    override val imaginary: FloatValue
        get() = FloatValue(this.data[1])

    override val logicalSize: Int
        get() = -1

    /**
     * Compares this [Complex32Value] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [Complex32Value] can only be compared to other [NumericValue]s.
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
        is Complex32Value -> { /* Mathematically, this is not sound. But for the purpose of sorting, this should be sufficient. */
            val real = this.real.compareTo(other.real)
            if (real == 0) {
                real + this.imaginary.compareTo(other.imaginary)
            } else {
                real
            }
        }
        is Complex64Value -> { /* Mathematically, this is not sound. But for the purpose of sorting, this should be sufficient. */
            val real = this.real.compareTo(other.real)
            if (real == 0) {
                real + this.imaginary.compareTo(other.imaginary)
            } else {
                real
            }
        }
        else -> throw IllegalArgumentException("LongValues can only be compared to other numeric values.")
    }

    /**
     * Checks for equality between this [Complex32Value] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [Complex32Value] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is Complex32Value) && (this.data.contentEquals(other.data))

    override fun asComplex32(): Complex32Value = this
    override fun asComplex64(): Complex64Value = Complex64Value(this.data[0], data[1])

    /**
     * Calculates and returns the inverse of this [Complex32Value].
     *
     * @return The inverse [Complex32Value].
     */
    override fun inverse(): Complex32Value = Complex32Value((this.data[0] / (this.data[0] * this.data[0] + this.data[1] * this.data[1])), -(this.data[1] / (this.data[0] * this.data[0] + this.data[1] * this.data[1])))

    /**
     * Returns the complex conjugate of this [Complex32Value]
     *
     * @return The conjugate [Complex32Value].
     */
    override fun conjugate(): Complex32Value = Complex32Value(this.data[0], -this.data[1])

    override fun unaryMinus() = Complex32Value(-this.data[0], -this.data[1])

    override fun plus(other: NumericValue<*>) = when (other) {
        is Complex32Value -> Complex32Value(this.data[0] + other.data[0], this.data[1] + other.data[1])
        is Complex64Value -> Complex32Value(this.data[0] + other.data[0], this.data[1] + other.data[1])
        else -> Complex32Value(this.data[0] + other.real.asDouble().value, this.data[1])
    }

    override fun minus(other: NumericValue<*>) = when (other) {
        is Complex32Value -> Complex32Value(this.data[0] - other.data[0], this.data[1] - other.data[1])
        is Complex64Value -> Complex32Value(this.data[0] - other.data[0], this.data[1] - other.data[1])
        else -> Complex32Value(this.data[0] - other.real.asDouble().value, this.data[1])
    }

    override fun times(other: NumericValue<*>) = when (other) {
        is Complex32Value -> Complex32Value(this.data[0] * other.data[0] - this.data[1] * other.data[1], this.data[0] * other.data[1] + this.data[1] * other.data[0])
        is Complex64Value -> Complex32Value(this.data[0] * other.data[0] - this.data[1] * other.data[1], this.data[0] * other.data[1] + this.data[1] * other.data[0])
        else -> Complex32Value(this.data[0] * other.real.asDouble().value, this.data[1] * other.real.asDouble().value)
    }

    override fun div(other: NumericValue<*>): Complex32Value = when (other) {
        is Complex32Value -> {
            val c = other.data[0]
            val d = other.data[1]
            if (kotlin.math.abs(c) < kotlin.math.abs(d)) {
                val q = c / d
                val denominator = c * q + d
                Complex32Value((this.data[0] * q + this.data[1]) / denominator, (this.data[1] * q - this.data[0]) / denominator)
            } else {
                val q = d / c
                val denominator = d * q + c
                Complex32Value((this.data[1] * q + this.data[0]) / denominator, (this.data[1] - this.data[0] * q) / denominator)
            }
        }
        is Complex64Value -> {
            if (kotlin.math.abs(other.data[0]) < kotlin.math.abs(other.data[1])) {
                val q = other.data[0] / other.data[1]
                val denominator = other.data[0] * q + other.data[1]
                Complex32Value((this.data[0] * q + this.data[1]) / denominator, (this.data[1] * q - this.data[0]) / denominator)
            } else {
                val q = other.data[1] / other.data[0]
                val denominator = other.data[1] * q + other.data[0]
                Complex32Value((this.data[1] * q + this.data[0]) / denominator, (this.data[1] - this.data[0] * q) / denominator)
            }
        }
        else -> {
            val c = other.asFloat().value
            Complex32Value(this.data[0] / c, this.data[1] / c)
        }
    }

    override fun abs() = FloatValue(kotlin.math.sqrt(this.data[0] * this.data[0] + this.data[1] * this.data[1]))

    override fun pow(x: Double): Complex64Value {
        val real = x * kotlin.math.ln(this.abs().value)
        val imaginary = x * kotlin.math.atan2(this.data[1], this.data[0])
        val exp = kotlin.math.exp(real)
        return Complex64Value(exp * kotlin.math.cos(imaginary), exp * kotlin.math.sin(imaginary))
    }

    override fun pow(x: Int): Complex64Value {
        val real = x * kotlin.math.ln(this.abs().value)
        val imaginary = x * kotlin.math.atan2(this.data[1], this.data[0])
        val exp = kotlin.math.exp(real)
        return Complex64Value(exp * kotlin.math.cos(imaginary), exp * kotlin.math.sin(imaginary))
    }

    override fun exp(): Complex64Value {
        val expReal = kotlin.math.exp(this.data[0])
        return Complex64Value(expReal * kotlin.math.cos(this.data[1]), expReal * kotlin.math.sin(this.data[1]))
    }

    override fun ln() = Complex64Value(kotlin.math.ln(this.abs().value), kotlin.math.atan2(this.data[1], this.data[0]))

    override fun sqrt(): Complex64Value = pow(1.0 / 2.0)

    override fun cos(): Complex64Value = Complex64Value(kotlin.math.cos(this.data[0]) * kotlin.math.cosh(this.data[1]), -kotlin.math.sin(this.data[0]) * kotlin.math.sinh(this.data[1]))
    override fun sin(): Complex64Value = Complex64Value(kotlin.math.sin(this.data[0]) * kotlin.math.cosh(this.data[1]), kotlin.math.cos(this.data[0]) * kotlin.math.sinh(this.data[1]))

    override fun tan(): Complex64Value {
        if (this.data[1] > 20.0) {
            return Complex64Value(0.0, 1.0)
        }
        if (this.data[1] < -20.0) {
            return Complex64Value(0.0, -1.0)
        }

        val r: Double = 2.0 * this.data[0]
        val i: Double = 2.0 * this.data[1]
        val d = kotlin.math.cos(r) + kotlin.math.cosh(i)

        return Complex64Value(kotlin.math.sin(r) / d, kotlin.math.sinh(i) / d)
    }

    override fun atan(): Complex64Value = ((this + I) / (I - this)).ln() * (I / Complex32Value(floatArrayOf(2.0f, 0.0f)))
}