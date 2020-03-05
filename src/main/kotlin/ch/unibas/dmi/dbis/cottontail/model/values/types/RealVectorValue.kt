package ch.unibas.dmi.dbis.cottontail.model.values.types
/**
 * Represents a real valued vector value of any primitive type, i.e. [Short], [Int], [Long], [Float]
 * or [Double]. This  is an abstraction over the existing primitive array types provided by Kotlin.
 * It allows for the advanced type system implemented by Cottontail DB.
 *
 * @version 1.2
 * @author Ralph Gasser
 */
interface RealVectorValue<T: Number> : VectorValue<T>, Iterable<RealValue<T>> {
    /**
     * Returns the i-th entry of this [RealVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): RealValue<T>

    /**
     * Creates and returns an [Iterator] for the values held by this [VectorValue].
     */
    override fun iterator(): Iterator<RealValue<T>> = object : Iterator<RealValue<T>> {
        var index = 0
        override fun hasNext(): Boolean = this.index < this@RealVectorValue.logicalSize
        override fun next(): RealValue<T> = this@RealVectorValue[this.index++]
    }
}