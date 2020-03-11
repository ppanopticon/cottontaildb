package ch.unibas.dmi.dbis.cottontail.model.values.types

/**
 * Represents a vector value of any type, i.e. a value that consists only more than one entry. Vector
 * values are always numeric! This  is an abstraction over the existing primitive array types provided
 * by Kotlin. It allows for the advanced type system implemented by Cottontail DB.
 *
 * @version 1.2
 * @author Ralph Gasser
 */
interface VectorValue<T: Number> : Value {
    /**
     * Returns the i-th entry of  this [VectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    operator fun get(i: Int): NumericValue<T>

    /**
     * Returns the i-th entry of  this [VectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    fun getAsBool(i: Int): Boolean

    /**
     * Returns true, if this [VectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [VectorValue] consists of all zeroes
     */
    fun allZeros(): Boolean

    /**
     * Returns true, if this [VectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [VectorValue] consists of all ones
     */
    fun allOnes(): Boolean

    /**
     * Returns the indices of this [VectorValue].
     *
     * @return The indices of this [VectorValue]
     */
    val indices: IntRange

    /**
     * Creates and returns an exact copy of this [VectorValue].
     *
     * @return Exact copy of this [VectorValue].
     */
    fun copy(): VectorValue<T>

    operator fun plus(other: VectorValue<*>): VectorValue<T>
    operator fun minus(other: VectorValue<*>): VectorValue<T>
    operator fun times(other: VectorValue<*>): VectorValue<T>
    operator fun div(other: VectorValue<*>): VectorValue<T>

    operator fun plus(other: NumericValue<*>): VectorValue<T>
    operator fun minus(other: NumericValue<*>): VectorValue<T>
    operator fun times(other: NumericValue<*>): VectorValue<T>
    operator fun div(other: NumericValue<*>): VectorValue<T>

    fun pow (x: Int): VectorValue<Double>
    fun sqrt(): VectorValue<Double>
    fun abs(): VectorValue<T>
    fun sum(): NumericValue<T>

    /**
     * Special implementation of the L1 / Manhattan distance. Can be overridden to create optimized versions of it.
     *
     * @param other The [VectorValue] to calculate the distance to.
     */
    fun distanceL1(other: VectorValue<*>): NumericValue<*> = ((other-this).abs()).sum()

    /**
     * Special implementation of the L2 / Euclidean distance. Can be overridden to create optimized versions of it.
     *
     * @param other The [VectorValue] to calculate the distance to.
     */
    fun distanceL2(other: VectorValue<*>): NumericValue<*> = ((other-this).pow(2)).sum().sqrt()

    /**
     * Special implementation of the LP / Minkowski distance. Can be overridden to create optimized versions of it.
     *
     * @param other The [VectorValue] to calculate the distance to.
     */
    fun distanceLP(other: VectorValue<*>, p: Int): NumericValue<*> = ((other-this).pow(p)).sum().pow(1.0/p)
}