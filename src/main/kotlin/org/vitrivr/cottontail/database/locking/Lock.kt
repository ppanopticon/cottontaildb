package org.vitrivr.cottontail.database.locking

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.model.basics.TransactionId
import java.util.*
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Represents a [Lock] on some object (usually a database object).
 *
 * Inspired by: https://github.com/dstibrany/LockManager
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Lock internal constructor(private val waitForGraph: WaitForGraph) {

    /** List of [LockHolder]s that hold a lock on this [Lock]. */
    private val owners = ObjectOpenHashSet<LockHolder>()

    /** Internal [ReentrantLock] used to mediate access to this [Lock] object. */
    private val lock = ReentrantReadWriteLock(true)

    /** Internal [Condition] used to signal waiting threads that they can proceed. */
    private val waiters: Condition = this.lock.writeLock().newCondition()

    /** Number of shared locks held by this [Lock]. */
    @Volatile
    var sharedLockCount = 0
        private set

    /** Returns true, if this [Lock] is exclusively locked, false otherwise. */
    @Volatile
    var isExclusivelyLocked = false
        private set

    /** Returns true, if this [Lock] is shared locked, false otherwise. */
    val isSharedLocked
        get() = this.sharedLockCount > 0

    /**
     * Acquires a hold on this [Lock].
     *
     * @param txn [LockHolder] that tries to acquire the lock.
     * @param lockMode The [LockMode] the transaction tries to acquire.
     */
    fun acquire(txn: LockHolder, lockMode: LockMode) {
        when (lockMode) {
            LockMode.SHARED -> acquireSharedLock(txn)
            LockMode.EXCLUSIVE -> acquireExclusiveLock(txn)
            else -> throw IllegalArgumentException("Lock mode of type UNLOCKED cannot be acquired!")
        }
    }

    /**
     * Releases a hold on this [Lock].
     *
     * @param txn [LockHolder] that tries to release the lock.
     */
    fun release(txn: LockHolder) = this.lock.write {
        if (this.sharedLockCount > 0) {
            this.sharedLockCount--
        }
        if (this.isExclusivelyLocked) {
            this.isExclusivelyLocked = false
        }
        this.owners.remove(txn)
        this.waitForGraph.remove(txn)
        this.waiters.signalAll()
    }

    /**
     * Tries to upgrade the hold on this [Lock].
     *
     * @param txn [LockHolder] that tries to upgrade the [Lock].
     */
    fun upgrade(txn: LockHolder) = this.lock.write {
        if (this.owners.contains(txn) && isExclusivelyLocked) return
        while (this.isExclusivelyLocked || this.sharedLockCount > 1) {
            val ownersWithSelfRemoved: Set<LockHolder> = ObjectOpenHashSet(this.owners.filter { it != txn })
            this.waitForGraph.add(txn, ownersWithSelfRemoved)
            this.waitForGraph.detectDeadlock(txn)
            this.waiters.await()
        }
        this.sharedLockCount = 0
        this.isExclusivelyLocked = true
    }

    /**
     * Returns an unmodifiable set of all [TransactionId]s that currently have a hold on this [Lock]
     */
    fun getOwners(): Set<LockHolder> = Collections.unmodifiableSet(this.owners)

    /**
     * Returns the [LockMode] for this [Lock].
     *
     * @return The [LockManager] for this [Lock].
     */
    fun getMode(): LockMode = this.lock.read {
        when {
            this.isExclusivelyLocked -> LockMode.EXCLUSIVE
            this.isSharedLocked -> LockMode.SHARED
            else -> LockMode.NO_LOCK
        }
    }

    /**
     * Tries to acquire a shared lock on this [Lock]. Blocks until the lock could be acquired,
     *
     * @param txn [LockHolder] that tries to acquire the lock.
     */
    private fun acquireSharedLock(txn: LockHolder) = this.lock.write {
        while (this.isExclusivelyLocked || this.lock.hasWaiters(waiters)) {
            this.waitForGraph.add(txn, this.owners)
            this.waitForGraph.detectDeadlock(txn)
            this.waiters.await()
        }
        this.sharedLockCount++
        this.owners.add(txn)
    }

    /**
     * Tries to acquire an exclusive lock on this [Lock]. Blocks until the lock could be acquired,
     *
     * @param txn [LockHolder] that tries to acquire the exclusive lock.
     */
    private fun acquireExclusiveLock(txn: LockHolder) = this.lock.write {
        while (this.isExclusivelyLocked || this.isSharedLocked) {
            this.waitForGraph.add(txn, this.owners)
            this.waitForGraph.detectDeadlock(txn)
            this.waiters.await()
        }
        this.isExclusivelyLocked = true
        this.owners.add(txn)
    }
}