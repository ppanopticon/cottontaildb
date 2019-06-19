package ch.unibas.dmi.dbis.cottontail.math.knn

import java.util.*
import kotlin.collections.ArrayList

/**
 * A [HeapSelect] implementation inspired by the Smile library (@see https://github.com/haifengl/smile)
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class HeapSelect<T: Comparable<T>> (val k: Int) {

    /** Indicates whether this [HeapSelect] is currently sorted or not. */
    @Volatile
    var sorted: Boolean = true
        private set

    /** Returns the number of items that have been added to this [HeapSelect] so far. */
    @Volatile
    var added: Int = 0
        private set

    /** Returns the current size of this [HeapSelect], i.e. the number of items contained in the heap. */
    val size
        @Synchronized get() = Math.min(this.k, this.added)

    /** The array list containing the heap for this [HeapSelect]. */
    private val heap = ArrayList<T>(k)


    /**
     * Adds a new element to this [HeapSelect].
     *
     * @param data The element to add to this [HeapSelect].
     */
    @Synchronized
    fun add(data: T) {
        sorted = false
        if (this.added < this.k) {
            this.heap.add(data)
            this.added += 1
            if (this.added == k) {
                heapify()
            }
        } else {
            this.added += 1
            if (data < this.heap[0]) {
                this.heap[0] = data
                siftDown(0, this.k-1)
            }
        }
    }

    /**
     * Returns the smallest value seen so far.
     *
     * @return Smallest value seen so far.
     */
    @Synchronized
    fun peek(): T? {
        return this.heap[0]
    }

    /**
     * Returns the i-*th* smallest value seen so far. i = 0 returns the smallest
     * value seen, i = 1 the second largest, ..., i = k-1 the last position
     * tracked. Also, i must be less than the number of previous assimilated.
     */
    @Synchronized
    operator fun get(i: Int): T {
        if (i > Math.min(k, this.added) - 1) {
            throw IllegalArgumentException("HeapSelect i is greater than the number of data received so far.")
        }

        if (i == this.k - 1) {
            return this.heap[0]
        }

        if (!this.sorted) {
            this.sort()
        }

        return this.heap[this.k - 1 - i]
    }

    /**
     * Returns an unmodifiable version of the heap. The list returned by this function is backed by the heap and changes
     * to the heap will cause the list to change.
     *
     * @return Unmodifiable version of the heap.
     */
    @Synchronized
    fun heap(): List<T> = Collections.unmodifiableList(this.heap)

    /**
     * Sorts the heap so that the smallest values move to the top of the [HeapSelect].
     */
    private fun sort() {
        val n = Math.min(this.k, this.added)
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
        sorted = true
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
        for (i in n / 2 - 1 downTo 0)
            siftDown(i, n-1)
    }
}