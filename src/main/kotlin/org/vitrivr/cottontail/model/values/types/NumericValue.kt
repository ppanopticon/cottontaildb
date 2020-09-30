package org.vitrivr.cottontail.model.values.types

import org.vitrivr.cottontail.model.values.*

/**
 * Represent a numeric value regardless of whether that [NumericValue] is real or complex. This is
 * an abstraction over the existing primitive types provided  by Kotlin. It allows for the advanced
 * type system implemented by Cottontail DB.
 *
 * @version 1.1
 * @author Ralph Gasser
 */
interface NumericValue<T : Number> : ScalarValue<T>, Comparable<NumericValue<T>> {

    /** Real part of this [NumericValue]  */
    val real: RealValue<T>

    /** Imaginary part of this [NumericValue]. */
    val imaginary: RealValue<T>

    fun asDouble(): DoubleValue
    fun asFloat(): FloatValue
    fun asLong(): LongValue
    fun asInt(): IntValue
    fun asShort(): ShortValue
    fun asByte(): ByteValue
    fun asComplex32(): Complex32Value
    fun asComplex64(): Complex64Value

    operator fun unaryMinus(): NumericValue<T>
    operator fun plus(other: NumericValue<*>): NumericValue<T>
    operator fun minus(other: NumericValue<*>): NumericValue<T>
    operator fun times(other: NumericValue<*>): NumericValue<T>
    operator fun div(other: NumericValue<*>): NumericValue<T>

    /**
     * Calculates and returns the absolute value of this [NumericValue].
     *
     * @return Absolute value of this [NumericValue].
     */
    fun abs(): NumericValue<T>

    /**
     * Calculates and returns this [NumericValue] raised to the power of the given [Int].
     * Can cause an implicit cast to [NumericValue].
     *
     * @param x Exponent for the operation.
     * @return This [NumericValue] raised to the power of x.
     */
    fun pow(x: Int): NumericValue<*>

    /**
     * Calculates and returns the square root of this [NumericValue]. Can cause an implicit cast.
     *
     * @return The square root of this [NumericValue].
     */
    fun sqrt(): NumericValue<*>

    /**
     * Calculates and returns this [NumericValue] raised to the power of the given [Double].
     * Causes an implicit cast to [NumericValue] of a [Double]
     *
     * @param x Exponent for the operation.
     * @return This [NumericValue] raised to the power of x.
     */
    fun pow(x: Double): NumericValue<Double>

    /**
     * Calculates and returns exponential function of this [NumericValue], i.e., e^(this). Causes an
     * implicit cast to [NumericValue] of a [Double]
     *
     * @return The exponential function of this [NumericValue].
     */
    fun exp(): NumericValue<Double>

    /**
     * Calculates and returns natural logarithm of this [NumericValue], i.e., ln(this). Causes an
     * implicit cast to [NumericValue] of a [Double]
     *
     * @return The natural logarithm of this [NumericValue].
     */
    fun ln(): NumericValue<Double>

    /**
     * Calculates and returns cosine-function of this [NumericValue], i.e., cos(this). Causes an
     * implicit cast to [NumericValue] of a [Double]
     *
     * @return The cosine of this [NumericValue].
     */
    fun cos(): NumericValue<Double>

    /**
     * Calculates and returns sine-function of this [NumericValue], i.e., sin(this). Causes an
     * implicit cast to [NumericValue] of a [Double]
     *
     * @return The sine of this [NumericValue].
     */
    fun sin(): NumericValue<Double>

    /**
     * Calculates and returns tangens-function of this [NumericValue], i.e., tan(this). Causes an
     * implicit cast to [NumericValue] of a [Double]
     *
     * @return The tangens of this [NumericValue].
     */
    fun tan(): NumericValue<Double>

    /**
     * Calculates and returns arctangens-function of this [NumericValue], i.e., atan(this). Causes an
     * implicit cast to [NumericValue] of a [Double]
     *
     * @return The arctangens n of this [NumericValue].
     */
    fun atan(): NumericValue<Double>

    /**
     * Comparison operator between a [NumericValue] and a [Number]. Returns -1, 0 or 1 of other value
     * is smaller, equal or greater than this value.
     *
     * @param other [Number] to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    operator fun compareTo(other: Number): Int
}