package org.vitrivr.cottontail.database.locking

import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [LockManager] implementation that allows for management of [Lock]s on [DBO]s.
 *
 * Inspired by: https://github.com/dstibrany/LockManager
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LockManager {

    /** List of all [Lock]s managed by this [LockManager]. */
    private val locks = Object2ObjectMaps.synchronize(Object2ObjectOpenHashMap<DBO, Lock>())

    /** The [WaitForGraph] data structure used to detect deadlock situations. */
    private val waitForGraph: WaitForGraph = WaitForGraph()

    /**
     * Returns an list all [DBO] [Name]s that are currently locked.
     *
     * @return List of all [DBO]s that are currently locked.
     */
    fun allLocks(): List<Pair<Name, Lock>> = this.locks.map { v -> v.key.name to v.value }

    /**
     * Tries to acquire a lock on [Name] for the given [LockHolder].
     *
     * @param txn [LockHolder] to acquire the lock for.
     * @param dbo [DBO] of the object to acquire a lock on.
     * @param mode The [LockMode]
     */
    fun lock(txn: LockHolder, dbo: DBO, mode: LockMode) = synchronized(dbo) {
        require(mode != LockMode.NO_LOCK) { "Cannot acquire a lock of mode $mode; try LockManager.release()." }
        val lockOn = lockOn(txn, dbo)
        if (lockOn === mode) {
            return
        } else if (lockOn === LockMode.EXCLUSIVE && mode === LockMode.SHARED) {
            return
        } else if (lockOn === LockMode.SHARED && mode === LockMode.EXCLUSIVE) {
            val lock: Lock = this.locks.computeIfAbsent(dbo) { Lock(this.waitForGraph) }
            lock.upgrade(txn)
            txn.addLock(lock)
        } else {
            val lock: Lock = this.locks.computeIfAbsent(dbo) { Lock(this.waitForGraph) }
            lock.acquire(txn, mode)
            txn.addLock(lock)
        }
    }

    /**
     * Unlocks the lock on [Name] held by the given [LockHolder].
     *
     * @param txn [LockHolder] to release the lock for.
     * @param dbo [DBO] of the object to release the lock on.
     */
    fun unlock(txn: LockHolder, dbo: DBO) = synchronized(dbo) {
        this.locks.computeIfPresent(dbo) { _, lock ->
            lock.release(txn)
            txn.removeLock(lock)
            if (lock.getMode() === LockMode.NO_LOCK) {
                null
            } else {
                lock
            }
        }
    }

    /**
     * Returns the [LockMode] the given [LockHolder] has on the given [DBO]. If it holds
     * no lock, then [LockMode.NO_LOCK] is returned.
     *
     * @param txn The [LockHolder] to check.
     * @param dbo The [DBO] to check.
     * @return [LockMode]
     */
    fun lockOn(txn: LockHolder, dbo: DBO): LockMode {
        return txn.getLocks().find { it == this.locks[dbo] }?.getMode() ?: LockMode.NO_LOCK
    }
}