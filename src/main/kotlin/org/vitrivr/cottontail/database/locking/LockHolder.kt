package org.vitrivr.cottontail.database.locking

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.execution.TransactionManager.Transaction
import org.vitrivr.cottontail.model.basics.TransactionId
import java.util.*

/**
 * Represents a holder of one or multiple [Lock]s.
 *
 * Inspired by: https://github.com/dstibrany/LockManager
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
open class LockHolder(val txId: TransactionId) : Comparable<LockHolder> {
    /** The [Lock]s held by this [Transaction]. */
    protected val locks = ObjectOpenHashSet<Lock>()

    /** Returns the number of [Lock]s held by this [LockHolder]. */
    val numberOfLocks: Int
        get() = this.locks.size

    /**
     * Accessor for all [Lock]s held by this [LockHolder].
     *
     * @return Set of [Lock]s currently held by this [LockHolder].
     */
    fun getLocks(): Set<Lock> = Collections.unmodifiableSet(this.locks)

    /**
     * Adds a [Lock] to the list of [Lock]s held by this [LockHolder].
     *
     * @param lock The [Lock] that should be added.
     */
    internal fun addLock(lock: Lock) {
        this.locks.add(lock)
    }

    /**
     * Removes a [Lock] from the list of [Lock]s held by this [LockHolder].
     *
     * @param lock The [Lock] that should be removed.
     */
    internal fun removeLock(lock: Lock) {
        this.locks.remove(lock)
    }

    /**
     * Compares this [LockHolder] to the other [LockHolder].
     */
    override operator fun compareTo(other: LockHolder): Int {
        return (this.txId - other.txId).toInt()
    }
}