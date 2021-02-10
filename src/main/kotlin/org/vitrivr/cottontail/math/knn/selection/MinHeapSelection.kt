package org.vitrivr.cottontail.math.knn.selection

import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.util.concurrent.locks.StampedLock
import kotlin.math.min

/**
 * This is a [Selection] implementation for nearest neighbor search (NNS). It selects the k smallest
 * [Comparable]s it has encountered in a stream of [Comparable]s.
 *
 * Implementation inspired by the Smile library (@see https://github.com/haifengl/smile)
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class MinHeapSelection<T : Comparable<T>>(override val k: Int) : Selection<T> {

    /** The [ArrayList] containing the heap for this [MinHeapSelection]. */
    private val heap = ArrayList<T>(this.k)

    /** Stamped lock that facilitates concurrent access to this [MinHeapSelection]. */
    private val accessLock = StampedLock()

    /** Indicates whether this [MinHeapSelection] is currently sorted or not. */
    @Volatile
    var sorted: Boolean = true
        private set

    /** Number of items that have been added to this [MinHeapSelection] so far. */
    @Volatile
    var added: Int = 0
        private set

    /** Returns the size of this [MinHeapSelection], i.e., the number of items contained in the heap. */
    override val size: Int
        get() = this.accessLock.read { this.heap.size }

    /**
     * Adds a new element to this [MinHeapSelection].
     *
     * @param element The element to add to this [MinHeapSelection].
     */
    override fun offer(element: T) = this.accessLock.write {
        this.sorted = false
        if (this.added < this.k) {
            this.heap.add(element)
            this.added += 1
            if (this.added == this.k) {
                heapify()
            }
        } else {
            this.added += 1
            if (element < this.heap[0]) {
                this.heap[0] = element
                siftDown(0, this.k - 1)
            }
        }
    }

    /**
     * Returns the largest (i.e. i == k-1) value retained by this [MinHeapSelection].
     *
     * @return Largest value seen so far.
     */
    override fun peek(): T? = this.accessLock.read {
        return this.heap.firstOrNull()
    }

    /**
     * Returns the i-*th* retained value seen  by this [MinHeapSelection]. i = 0 returns the
     * smallest value seen, i = 1 the second smallest, ..., i = k-1 the largest value tracked.
     * Also, i must be less than the number of previous assimilated.
     *
     * @param i The index of the value to return
     * @return The i-th smallest value retained by this [MinHeapSelection]
     */
    override operator fun get(i: Int): T = this.accessLock.write {
        val maxIdx = this.heap.size - 1
        require(i <= maxIdx) { "Index $i is out of bounds for this MinHeapSelect." }

        if (i == this.k - 1) {
            return this.heap[0]
        }

        if (!this.sorted) {
            this.sort()
        }

        val ret = this.heap[maxIdx - i]
        return ret
    }


    /**
     * Sorts the heap so that the smallest values move to the top of the [MinHeapSelection].
     */
    private fun sort() {
        val n = min(this.k, this.added)
        var inc = 1
        do {
            inc *= 3
            inc++
        } while (inc <= n)

        do {
            inc /= 3
            for (i in inc until n) {
                val v = this.heap[i]
                var j = i
                while (this.heap[j - inc] < v) {
                    this.heap[j] = this.heap[j - inc]
                    j -= inc
                    if (j < inc) {
                        break
                    }
                }
                this.heap[j] = v
            }
        } while (inc > 1)
        this.sorted = true
    }

    /**
     *
     */
    private fun siftDown(i: Int, n: Int) {
        var k = i
        while (2 * k <= n) {
            var j = 2 * k
            if (j < n && this.heap[j] < this.heap[j + 1]) {
                j++
            }
            if (this.heap[k] >= this.heap[j]) {
                break
            }

            /* Swap elements. */
            val a = this.heap[k]
            this.heap[k] = this.heap[j]
            this.heap[j] = a
            k = j
        }
    }

    /**
     *
     */
    private fun heapify() {
        val n = this.heap.size
        for (i in n / 2 - 1 downTo 0) siftDown(i, n - 1)
    }
}
