package org.vitrivr.cottontail.model.values.types

import org.vitrivr.cottontail.model.values.*

/**
 * Represent a complex c = a + ib, where a is the real part, be is the imaginary part and i the
 * complex constants. a, b can be  made up of primitive types such as [Short], [Int], [Long], [Float]
 * or [Double]. This is  an abstraction over the existing primitive types provided  by Kotlin. It
 * allows for the advanced type system implemented by Cottontail DB.
 *
 * @version 1.1
 * @author Ralph Gasser
 */
interface ComplexValue<T: Number>: NumericValue<T> {
    /** Returns the inverse of this [ComplexValue]. */
    fun inverse(): ComplexValue<T>

    /** Returns the complex conjugate of this [ComplexValue]. */
    fun conjugate(): ComplexValue<T>

    override fun asDouble(): DoubleValue = this.abs().asDouble()
    override fun asFloat(): FloatValue = this.abs().asFloat()
    override fun asLong(): LongValue = this.abs().asLong()
    override fun asInt(): IntValue = this.abs().asInt()
    override fun asShort(): ShortValue = this.abs().asShort()
    override fun plus(other: NumericValue<*>): ComplexValue<T>
    override fun minus(other: NumericValue<*>): ComplexValue<T>
    override fun times(other: NumericValue<*>): ComplexValue<T>
    override fun div(other: NumericValue<*>): ComplexValue<T>
    override fun asByte(): ByteValue = this.abs().asByte()
}