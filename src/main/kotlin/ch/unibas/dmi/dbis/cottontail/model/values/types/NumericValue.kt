package ch.unibas.dmi.dbis.cottontail.model.values.types

import ch.unibas.dmi.dbis.cottontail.model.values.*

/**
 * Represent a numeric value regardless of whether that [NumericValue] is real or complex. This is
 * an abstraction over the existing primitive types provided  by Kotlin. It allows for the advanced
 * type system implemented by Cottontail DB.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
interface NumericValue<T: Number> : ScalarValue<T> {

    /** Real part of this [ComplexValue] */
    val real: RealValue<T>

    /** Imaginary part of this [ComplexValue] */
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

    fun pow (x: Double): NumericValue<Double>
    fun pow (x: Int): NumericValue<Double>
    fun sqrt(): NumericValue<Double>
    fun abs(): NumericValue<Double>

    /**
     * Comparison operator between a [NumericValue] and a [Number]. Returns -1, 0 or 1 of other value
     * is smaller, equal or greater than this value.
     *
     * @param other [Number] to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    operator fun compareTo(other: Number): Int
}