package org.vitrivr.cottontail.storage.engine.hare.disk.wal

import org.vitrivr.cottontail.storage.engine.hare.disk.FileUtilities
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.wal.WALHareDiskManager.UndoLog
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.shared
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock
import java.util.zip.CRC32C

/**
 * A file used for write-ahead logging in [WALHareDiskManager]s. It's basically a page-level undo log.
 *
 * @see WALHareDiskManager
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
internal abstract class WriteAheadLog(protected val path: Path, protected val lockTimeout: Long = 5000L) : AutoCloseable {

    companion object {
        const val OFFSET_WAL_HEADER = 0L
    }

    /** [FileChannel] used to write to this [UndoLog]. If the desired file doesn't exist yet, it will be created. */
    protected val fileChannel = FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC, StandardOpenOption.CREATE)

    /** Acquire lock on [WriteAheadLog] file. */
    protected val fileLock = FileUtilities.acquireFileLock(this.fileChannel, this.lockTimeout)

    /** Reference to the [WALHeader] view used to access the header of this [WriteAheadLog] file. */
    protected val walHeader = WALHeader()

    /** Reference to the [WALEntry] view used to access the entries in this [WriteAheadLog] file. */
    protected val entry = WALEntry()

    /** A [ReentrantReadWriteLock] that mediates access to the closed state of this [HareDiskManager]. */
    protected val closeLock = StampedLock()

    /** Returns [WALState] of this [UndoLog] */
    val state: WALState
        get() {
            this.walHeader.read(this.fileChannel, OFFSET_WAL_HEADER)
            return this.walHeader.state
        }

    init {
        /* Make necessary preparations for opening the file. */
        this.prepareOpen()

        /* Move position to end of file. */
        this.fileChannel.position(this.fileChannel.size())
    }

    /**
     * Logs a COMMIT action and thus finalizes this [WriteAheadLog] file.
     */
    @Synchronized
    fun logCommit() = this.closeLock.shared {
        check(this.fileChannel.isOpen) { "HARE Write Ahead Log (WAL) file log failed: Channel has been closed and cannot be used (name = ${this.path.fileName})." }
        check(!this.walHeader.state.isSealed) { "HARE Write Ahead Log (WAL) file log failed: Log has been sealed (name = ${this.path.fileName})." }

        /* Calculate CRC32 checksum. */
        val crc32c = this.calculateChecksum()

        /* Update WAL header. */
        this.walHeader.checksum = crc32c
        this.walHeader.state = WALState.COMMITTED
        this.walHeader.write(this.fileChannel, OFFSET_WAL_HEADER)
    }

    /**
     * Logs an ABORT action and thus finalizes this [WriteAheadLog] file.
     */
    @Synchronized
    fun logAbort() = this.closeLock.shared {
        check(this.fileChannel.isOpen) { "HARE Write Ahead Log (WAL) file log failed: Channel has been closed and cannot be used (name = ${this.path.fileName})." }
        check(!this.walHeader.state.isSealed) { "HARE Write Ahead Log (WAL) file log failed: Log has been sealed (name = ${this.path.fileName})." }

        /* Calculate CRC32 checksum. */
        val crc32c = this.calculateChecksum()

        /* Update WAL header. */
        this.walHeader.checksum = crc32c
        this.walHeader.state = WALState.ABORTED
        this.walHeader.write(this.fileChannel, OFFSET_WAL_HEADER)
    }

    /**
     * Checks if this [WriteAheadLog] is valid.
     */
    @Synchronized
    fun valid(): Boolean = this.closeLock.shared {
        check(this.fileChannel.isOpen) { "HARE Write Ahead Log (WAL) file validation failed: Channel has been closed and cannot be used (name = ${this.path.fileName})." }

        /* Calculate CRC32 checksum. */
        val crc32c = this.calculateChecksum()

        /* Compare checksum in header with calculated checksum. */
        this.walHeader.read(this.fileChannel, OFFSET_WAL_HEADER)
        return crc32c == this.walHeader.checksum
    }

    /**
     * Closes this [UndoLog].
     */
    @Synchronized
    override fun close() = this.closeLock.exclusive {
        if (this.fileChannel.isOpen) {
            this.fileLock.release()
            this.fileChannel.close()
        }
    }

    /**
     * Deletes this [UndoLog] file. Calling this method also closes the associated [FileChannel].
     */
    @Synchronized
    fun delete() = this.closeLock.exclusive {
        if (this.fileChannel.isOpen) {
            this.fileLock.release()
            this.fileChannel.close()
        }
        Files.delete(this.path)
    }

    /**
     *
     */
    protected abstract fun prepareOpen()

    /**
     * Re-calculates the [CRC32C] checksum for this [WriteAheadLog] and updates its [crc32] field.
     */
    protected abstract fun calculateChecksum(): Long
}