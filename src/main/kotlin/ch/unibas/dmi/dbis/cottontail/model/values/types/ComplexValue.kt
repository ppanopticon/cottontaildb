package ch.unibas.dmi.dbis.cottontail.model.values.types

import ch.unibas.dmi.dbis.cottontail.model.values.*

/**
 * Represent a complex c = a + ib, where a is the real part, be is the imaginary part and i the
 * complex constants. a, b can be  made up of primitive types such as [Short], [Int], [Long], [Float]
 * or [Double]. This is  an abstraction over the existing primitive types provided  by Kotlin. It
 * allows for the advanced type system implemented by Cottontail DB.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
interface ComplexValue<T: Number>: NumericValue<T> {
    /** Returns the inverse of this [ComplexValue] */
    fun inverse(): ComplexValue<T>

    /** Returns the modulo of this [ComplexValue] */
    fun modulo(): RealValue<T>

    override fun asDouble(): DoubleValue = this.modulo().asDouble()
    override fun asFloat(): FloatValue = this.modulo().asFloat()
    override fun asLong(): LongValue = this.modulo().asLong()
    override fun asInt(): IntValue = this.modulo().asInt()
    override fun asShort(): ShortValue = this.modulo().asShort()
    override fun asByte(): ByteValue = this.modulo().asByte()
}