package org.vitrivr.cottontail.math.knn.selection

/**
 * A selection algorithm that returns k [Comparable]s from a stream of [Comparable]s.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Selection<T : Comparable<T>> {

    /** The number of elements this [Selection] can contain. */
    val k: Int

    /** The actual size of this [Selection]. Always size <= k*/
    val size: Int

    /** Offers an element of type [T] to this [Selection]. */
    fun offer(element: T)

    /** Returns the k-th smallest value encountered so far by this [Selection]. */
    fun peek(): T?

    /**
     * Returns the i-th value held by this [Selection].
     *
     * @param i The index of the desired value.
     * @return The value [T]
     *
     * @throws IllegalArgumentException If i > size
     */
    operator fun get(i: Int): T
}