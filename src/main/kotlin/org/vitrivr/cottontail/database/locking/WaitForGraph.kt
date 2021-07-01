package org.vitrivr.cottontail.database.locking

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * A data structure that can be used to detect deadlock situations.
 *
 * Inspired by: https://github.com/dstibrany/LockManager
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class WaitForGraph {
    private val adjacencyList = Collections.synchronizedMap(Object2ObjectOpenHashMap<LockHolder<*>, ObjectOpenHashSet<LockHolder<*>>>())
    private val rwl = ReentrantReadWriteLock()
    private val sharedLock: ReentrantReadWriteLock.ReadLock = this.rwl.readLock()


    /** The */
    private val exclusiveLock: ReentrantReadWriteLock.WriteLock = this.rwl.writeLock()

    /**
     * Adds a [LockHolder] to this [WaitForGraph].
     *
     * @param
     */
    fun add(predecessor: LockHolder<*>, successors: Set<LockHolder<*>>?) {
        this.sharedLock.lock()
        try {
            val txnList = this.adjacencyList.getOrDefault(predecessor, ObjectOpenHashSet<LockHolder<*>>())
            txnList.addAll(successors!!)
            this.adjacencyList[predecessor] = txnList
        } finally {
            this.sharedLock.unlock()
        }
    }

    /**
     *
     */
    fun remove(txn: LockHolder<*>) {
        this.sharedLock.lock()
        try {
            this.adjacencyList.remove(txn)
            removeSuccessor(txn)
        } finally {
            this.sharedLock.unlock()
        }
    }

    /**
     *
     */
    fun hasEdge(txn1: LockHolder<*>, txn2: LockHolder<*>): Boolean {
        val txnList = this.adjacencyList[txn1] ?: return false
        return txnList.contains(txn2)
    }

    /**
     *
     */
    fun findCycles(): List<List<LockHolder<*>>> {
        this.exclusiveLock.lock()
        return try {
            val dfs = DepthFirstSearch()
            dfs.start()
            dfs.getCycles()
        } finally {
            this.exclusiveLock.unlock()
        }
    }

    /**
     * Tries to detect deadlocks involing the given [LockHolder].
     *
     * @param currentTxn The [LockHolder] to check.
     * @throws [DeadlockException] If deadlock is detected.
     */
    @Throws(DeadlockException::class)
    fun detectDeadlock(currentTxn: LockHolder<*>) {
        val cycles = findCycles()
        for (cycleGroup in cycles) {
            if (cycleGroup.contains(currentTxn)) {
                throw DeadlockException(currentTxn, cycleGroup)
            }
        }
    }

    /**
     *
     */
    private fun removeSuccessor(txnToRemove: LockHolder<*>) {
        for (predecessor in adjacencyList.keys) {
            val successors = adjacencyList[predecessor]
            successors?.remove(txnToRemove)
        }
    }

    internal inner class DepthFirstSearch {
        private val visited: MutableSet<LockHolder<*>> = HashSet()
        private val cycles: MutableList<List<LockHolder<*>>> = ArrayList()
        fun start() {
            for (txn in adjacencyList.keys) {
                if (!visited.contains(txn)) {
                    visit(txn, ArrayList())
                }
            }
        }

        fun getCycles(): List<List<LockHolder<*>>> {
            return cycles
        }

        fun getVisited(): Set<LockHolder<*>> {
            return visited
        }

        private fun visit(node: LockHolder<*>, path: MutableList<LockHolder<*>>) {
            visited.add(node)
            path.add(node)
            if (adjacencyList.containsKey(node)) {
                for (neighbour in adjacencyList[node]!!) {
                    if (!visited.contains(neighbour)) {
                        visit(neighbour, ArrayList(path))
                    } else {
                        if (path.contains(neighbour)) {
                            cycles.add(getCycleFromPath(path, neighbour))
                        }
                    }
                }
            }
        }

        private fun getCycleFromPath(path: List<LockHolder<*>>, target: LockHolder<*>): List<LockHolder<*>> {
            return path.subList(path.indexOf(target), path.size)
        }
    }
}