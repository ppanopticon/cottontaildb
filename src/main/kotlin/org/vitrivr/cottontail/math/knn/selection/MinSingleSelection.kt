package org.vitrivr.cottontail.math.knn.selection

import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.util.concurrent.locks.StampedLock

/**
 * This is a special [Selection] implementation for nearest neighbor search (NNS), i.e., a kNN
 * implementation with k = 1. It selects the smallest [Comparable] from a stream of [Comparable]s
 * it has encountered.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class MinSingleSelection<T : Comparable<T>> : Selection<T> {

    /** Internal pointer to the smallest elements tracked by this [MinSingleSelection]. */
    @Volatile
    private var element: T? = null

    /** Internal lock for concurrent access. */
    private val lock = StampedLock()

    /** Constant k value for this [MinSingleSelection]. */
    override val k: Int = 1

    /** Size of this [MinSingleSelection], which is either 1 or 0. */
    override val size: Int
        get() = this.lock.read {
            if (this.element != null) {
                1
            } else {
                0
            }
        }

    override fun offer(element: T) = this.lock.write {
        val current = this.element
        if (current == null || current > element) {
            this.element = element
        }
    }

    /** Returns the smallest value seen so far by this [MinSingleSelection]. */
    override fun peek(): T? = this.lock.read { this.element }

    /**
     * Returns the i-th value held by this [MinSingleSelection].
     *
     * @param i The index of the desired value.
     * @return The value [T]
     *
     * @throws IllegalArgumentException If i > size
     */
    override fun get(i: Int): T = this.lock.read {
        require(i < this.size) { "Index $i is out of bounds for this MinSingleSelect." }
        this.element!!
    }
}