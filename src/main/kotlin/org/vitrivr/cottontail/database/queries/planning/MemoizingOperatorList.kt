package org.vitrivr.cottontail.database.queries.planning

import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.vitrivr.cottontail.database.queries.OperatorNode
import java.util.*

/**
 * This is a [Queue] like data structure that memorizes the [OperatorNode]s it has seen and
 * keeps them unique (i.e., a "known" [OperatorNode] cannot be enqueued anymore).
 *
 * Used during query planning.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class MemoizingOperatorList<T : OperatorNode>(vararg elements: T) {

    /** Internal list of [OperatorNode]s. */
    private val list = LinkedList<T>()

    /** Internal list of digests used to */
    private val digests = LongOpenHashSet()

    /** The number if elements in this [MemoizingOperatorList]. */
    val size: Int
        get() = this.list.size

    /** Whether this [MemoizingOperatorList] is currently empty. */
    val isEmpty: Boolean
        get() = this.list.isEmpty()

    init {
        for (e in elements) {
            this.enqueue(e)
        }
    }

    /**
     * Enqueues a [OperatorNode] in this [MemoizingOperatorList]. Only known
     *
     * @param node The [OperatorNode] to enqueue.
     * @param force If true, [OperatorNode] will be added even though it has already been seen by this [MemoizingOperatorList]
     * @return True on success, false if [OperatorNode] is already known.
     */
    fun enqueue(node: T, force: Boolean = false): Boolean {
        val digest = node.digest()
        if (force || !this.digests.contains(digest)) {
            this.list.add(node)
            this.digests.add(digest)
            return true
        }
        return false
    }

    /**
     * Removes and returns [OperatorNode] from this [MemoizingOperatorList].
     *
     * @return Next [OperatorNode] in queue or null if list is empty.
     */
    fun dequeue(): T? = this.list.poll()

    /**
     * Returns a [List] of all [OperatorNode]s contained in this [MemoizingOperatorList].
     *
     * @return [List] of [OperatorNode]s
     */
    fun toList(): List<T> = Collections.unmodifiableList(this.list)
}