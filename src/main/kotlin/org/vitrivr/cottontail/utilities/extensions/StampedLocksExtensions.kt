package org.vitrivr.cottontail.utilities.extensions

import java.util.concurrent.locks.StampedLock

/**
 * Executes the given [action] under the read lock of this [StampedLock].
 *
 * @param action The action to execute. Must be side-effect free and
 * @return the return value of the action.
 */
inline fun <T> StampedLock.read(action: () -> T): T {
    val stamp = this.readLock()
    try {
        return action()
    } catch (e: Throwable) {
        e.printStackTrace()
        throw e
    } finally {
        this.unlock(stamp)
    }
}

/**
 * Executes the given [action] under the (shared) read lock of this [StampedLock].
 *
 * @param action The action to execute. Must be side-effect free and
 * @return the return value of the action.
 */
inline fun <T> StampedLock.shared(action: () -> T) = read(action)

/**
 * Tries to execute the given [action] under and optimistic read lock of this [StampedLock]. If the optimistic lock
 * fails or a lock was acquired while executing the action, then a fallback to an ordinary read lock is used.
 *
 * @param action The action to execute. Must be idempotent and side-effect free.
 * @return the return value of the action.
 */
inline fun <T> StampedLock.optimisticRead(action: () -> T): T {
    val stamp = this.tryOptimisticRead()
    if (stamp == 0L) {
        return this.read(action)
    }
    val ret = action()
    return if (this.validate(stamp)) {
        ret
    } else {
        this.read(action)
    }
}

/**
 * Tries to execute the given [action] under and optimistic lock of this [StampedLock]. If the optimistic lock
 * fails or a lock was acquire while executing the action, then a fallback to an ordinary read lock is used.
 *
 * @param action The action to execute. Must be idempotent and side-effect free.
 * @return the return value of the action.
 */
inline fun <T> StampedLock.optimistic(action: () -> T): T = optimisticRead(action)


/**
 * Executes the given [action] under the write lock of this [StampedLock].
 *
 * @param action The action to execute. Must be side-effect free.
 * @return the return value of the action.
 */
inline fun <T> StampedLock.write(action: () -> T): T {
    val stamp = this.writeLock()
    try {
        return action()
    } catch (e: Throwable) {
        e.printStackTrace()
        throw e
    } finally {
        this.unlock(stamp)
    }
}

/**
 * Executes the given [action] under the (exclusive) write lock of this [StampedLock].
 *
 * @param action The action to execute. Must be side-effect free.
 * @return the return value of the action.
 */
inline fun <T> StampedLock.exclusive(action: () -> T): T = write(action)

/**
 * Executes the given [action] under the read lock of this [StampedLock].
 *
 * @param action The action to execute. Must be side-effect free.
 * @return the return value of the action.
 */
fun StampedLock.convertWriteLock(stamp: Long): Long {
    var new = this.tryConvertToWriteLock(stamp)
    while (new == 0L) {
        new = this.tryConvertToWriteLock(stamp)
        Thread.onSpinWait()
    }
    return new
}