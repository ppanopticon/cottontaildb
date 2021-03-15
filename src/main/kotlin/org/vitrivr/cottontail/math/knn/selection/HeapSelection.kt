package org.vitrivr.cottontail.math.knn.selection

import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.util.concurrent.locks.StampedLock
import kotlin.math.min

/**
 * A data structure used for heap based sorting.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class HeapSelection<T>(val k: Long, val comparator: Comparator<T>) {

    /** The [ArrayList] containing the heap for this [HeapSelection]. */
    private val heap = ObjectBigArrayBigList<T>(this.k)

    /** A lock that mediates access to this [HeapSelection]. */
    private val lock = StampedLock()

    /** Indicates whether this [MinHeapSelection] is currently sorted or not. */
    @Volatile
    var sorted: Boolean = true
        private set

    /** Number of items that have been added to this [HeapSelection] so far. */
    @Volatile
    var added: Long = 0
        private set

    /** Returns the size of this [HeapSelection], i.e., the number of items contained in the heap. */
    val size: Long
        get() = this.lock.read { this.heap.size64() }

    /**
     * Adds a new element to this [HeapSelection].
     *
     * @param element The element to add to this [HeapSelection].
     */
    fun offer(element: T) = this.lock.write {
        if ((this.added++) < this.k) {
            this.sorted = false
            this.heap.add(element)
            if (this.added == this.k) {
                heapify()
            }
        } else {
            if (this.comparator.compare(element, this.heap[0]) < 0) {
                this.heap[0] = element
                siftDown(0, this.k - 1)
            }
        }
    }

    /**
     * Returns the largest (i.e. i == k-1) value retained by this [HeapSelection].
     *
     * @return Largest value seen so far.
     */
    fun peek(): T? = this.lock.read { this.heap.firstOrNull() }

    /**
     * Returns the i-*th* retained value seen  by this [MinHeapSelection]. i = 0 returns the
     * smallest value seen, i = 1 the second smallest, ..., i = k-1 the largest value tracked.
     * Also, i must be less than the number of previous assimilated.
     *
     * @param i The index of the value to return
     * @return The i-th smallest value retained by this [MinHeapSelection]
     */
    operator fun get(i: Long): T = this.lock.read {
        val maxIdx = this.heap.size64() - 1
        require(i <= maxIdx) { "Index $i is out of bounds for this HeapSelect." }

        if (i == this.k - 1) {
            return this.heap[0]
        }

        if (!this.sorted) {
            this.sort()
        }

        return this.heap[maxIdx - i]
    }


    /**
     * Sorts the heap so that the smallest values move to the top of the [HeapSelection].
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
                while (this.comparator.compare(this.heap[j - inc], v) < 0) {
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
    private fun siftDown(i: Long, n: Long) {
        var k = i
        while (2 * k <= n) {
            var j = 2 * k
            if (j < n && this.comparator.compare(this.heap[j], this.heap[j + 1]) < 0) {
                j++
            }
            if (this.comparator.compare(this.heap[k], this.heap[j]) >= 0) break

            /* Swap elements. */
            val a = this.heap[k]
            this.heap[k] = this.heap[j]
            this.heap[j] = a
            k = j
        }
    }

    /**
     * Heapifies this [HeapSelection].
     */
    private fun heapify() {
        val n = this.heap.size64()
        for (i in n / 2 - 1 downTo 0) {
            siftDown(i, n - 1)
        }
    }
}