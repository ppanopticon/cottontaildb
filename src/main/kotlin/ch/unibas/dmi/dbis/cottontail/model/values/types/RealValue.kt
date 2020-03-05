package ch.unibas.dmi.dbis.cottontail.model.values.types

/**
 * Represent a real (i.e. non-complex) value containing a primitive type such as [Short], [Int],
 * [Long], [Float] or [Double]. This is an abstraction over the existing primitive types provided
 * by Kotlin. It allows for the advanced type system implemented by Cottontail DB.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
interface RealValue<T: Number>: NumericValue<T> {
    fun cos(): RealValue<T>
    fun sin(): RealValue<T>
    fun tan(): RealValue<T>
    fun atan(): RealValue<T>
}