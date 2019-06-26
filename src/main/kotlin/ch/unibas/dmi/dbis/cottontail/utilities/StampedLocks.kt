package ch.unibas.dmi.dbis.cottontail.utilities

import java.util.concurrent.locks.StampedLock

/**
 * Executes the given [action] under the read lock of this [StampedLock].
 *
 * @return the return value of the action.
 */
inline fun <T> StampedLock.read(action: () -> T): T {
    val stamp = this.readLock()
    try {
        return action()
    } finally {
        this.unlock(stamp)
    }
}

/**
 * Executes the given [action] under the read lock of this [StampedLock].
 *
 * @return the return value of the action.
 */
inline fun <T> StampedLock.write(action: () -> T): T {
    val stamp = this.writeLock()
    try {
        return action()
    } finally {
        this.unlock(stamp)
    }
}