package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.FileLockException
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.channels.FileLock

object FileUtilities {
    /**
     * Tries to acquire a file lock of this [FileChannel] and returns it.
     *
     * @param timeout The amount of milliseconds to wait for lock.
     * @return lock The [FileLock] acquired.
     */
    fun acquireFileLock(channel: FileChannel, timeout: Long) : FileLock {
        val start = System.currentTimeMillis()
        do {
            try {
                val lock = channel.tryLock()
                if (lock != null) {
                    return lock
                } else {
                    Thread.sleep(100)
                }
            } catch (e: IOException) {
                throw FileLockException("Could not open DiskManager for HARE file: failed to acquire file lock due to IOException.", e)
            }
        } while (System.currentTimeMillis() - start < timeout)
        throw FileLockException("Could not open DiskManager for HARE file: failed to acquire file lock due to timeout (time elapsed > ${timeout}ms).")
    }
}