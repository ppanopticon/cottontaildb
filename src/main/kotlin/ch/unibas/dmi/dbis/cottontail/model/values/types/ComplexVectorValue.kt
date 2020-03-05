package ch.unibas.dmi.dbis.cottontail.model.values.types
/**
 * Represents a complex valued [VectorValue] of any primitive type, i.e., a vector whose elements
 * consist of [ComplexValue]s. This  is an abstraction over the existing primitive array types
 * provided by Kotlin. It allows for the advanced type system implemented by Cottontail DB.
 *
 * @see VectorValue
 * @see ComplexValue
 *
 * @version 1.0
 * @author Ralph Gasser
 */
interface ComplexVectorValue<T: Number> : VectorValue<T>, Iterable<ComplexValue<T>> {
    /**
     * Returns the i-th entry of  this [VectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): ComplexValue<T>

    /**
     * Gets the real part of the i-th entry of this [ComplexVectorValue].
     *
     * @param i The index of the value to return.
     * @return The real component of the i-th entry in this [ComplexVectorValue]
     */
    fun real(i: Int): RealValue<T>

    /**
     * Gets the imaginary part of the i-th entry of this [ComplexVectorValue].
     *
     * @param i The index of the value to return.
     * @return The imaginary component of the i-th entry in this [ComplexVectorValue]
     */
    fun imaginary(i: Int): RealValue<T>

    /**
     * Creates and returns an [Iterator] for the values held by this [VectorValue].
     */
    override fun iterator(): Iterator<ComplexValue<T>> = object : Iterator<ComplexValue<T>> {
        var index = 0
        override fun hasNext(): Boolean = this.index < this@ComplexVectorValue.logicalSize
        override fun next(): ComplexValue<T> = this@ComplexVectorValue[this.index++]
    }
}