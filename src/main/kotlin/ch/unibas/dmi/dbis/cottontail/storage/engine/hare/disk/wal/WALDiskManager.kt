package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.wal

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.DataCorruptionException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.PageIdOutOfBoundException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.optimisticRead
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.read
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.locks.StampedLock

/**
 * The [WALDiskManager] facilitates reading and writing of [Page]s from/to the underlying disk storage. Only one
 * [DiskManager] can be opened per HARE file and it acquires an exclusive [FileLock] once created.
 *
 * As opposed to other [DiskManager] implementations, the [WALDiskManager] uses a write-ahead log (WAL) to make changes
 * to the underlying file. Upon commit or rollback, changes to the WAL are atomically transferred to the actual file. This
 * makes this implementation slower but offers some reliability circumstances that involve system crashes.
 *
 * @see DiskManager
 *
 * @version 1.0
 * @author Ralph Gasser
 */
class WALDiskManager(path: Path, lockTimeout: Long = 5000) : DiskManager(path, lockTimeout) {

    /** Reference to the [WriteAheadLog]. The [WriteAheadLog] is created whenever a write starts and removed on commit or rollback. */
    @Volatile
    private var wal: WriteAheadLog? = null

    /** A [StampedLock] to mediate access to [WriteAheadLog]. */
    private val lock = StampedLock()

    init {
        if (!this.header.isConsistent) {
            val walFile = this.path.parent.resolve("${this.path.fileName}.wal")
            if (Files.exists(walFile)) {
                this.wal = WriteAheadLog(walFile, this.lockTimeout)
            } else {
                throw DataCorruptionException("Ongoing transaction was detected but no WAL file found for HARE file ${this.path.fileName}.")
            }
        }
    }

    override fun read(id: PageId, page: Page): Unit = this.lock.optimisticRead {
        this.fileChannel.read(page.data.rewind(), this.pageIdToPosition(id))
    }

    override fun update(id: PageId, page: Page): Unit = this.lock.optimisticRead {
        withWAL { wal ->
            if (id > wal.maxPageId || id < 1) throw PageIdOutOfBoundException(id, this)
            wal.append(action = WALAction.UPDATE, id = id, page = page)
        }
    }

    override fun allocate(page: Page?): PageId = this.lock.read {
        withWAL { wal ->
            wal.append(action = WALAction.APPEND, page = page)
            wal.maxPageId
        }
    }

    override fun free(id: PageId) = this.lock.read {
        withWAL { wal ->
            wal.append(action = WALAction.FREE, id = id)
        }
    }

    /**
     * Performs a commit of all pending changes by replaying the [WriteAheadLog] file.
     */
    override fun commit(): Unit = this.lock.write {
        if (this.wal != null) {
            this.wal!!.replay { action, id, b ->
                when (action) {
                    WALAction.UPDATE -> this.fileChannel.write(b, this.pageIdToPosition(id))
                    WALAction.APPEND -> {
                        /* Default case; page has not been allocated yet */
                        if (id-1 == this.header.pages) {
                            this.header.pages++
                            this.header.flush()
                        }
                        this.fileChannel.write(b, this.pageIdToPosition(id))
                    }
                    WALAction.FREE -> TODO()
                }
                true
            }

            /* Update file header. */
            this.header.isConsistent = true
            this.header.checksum = this.calculateChecksum()
            this.header.flush()

            /** Delete WAL. */
            this.wal!!.close()
            this.wal!!.delete()
            this.wal = null
        }
    }

    /**
     * Performs a rollback of all pending changes by discarding the [WriteAheadLog] file.
     */
    override fun rollback() = this.lock.write {
        if (this.wal != null) {
            /* Update file header. */
            this.header.isConsistent = true
            this.header.flush()

            /** Delete WAL. */
            this.wal!!.close()
            this.wal!!.delete()
            this.wal = null
        }
    }

    /**
     * Deletes the HARE file backing this [WALDiskManager] and associated [WriteAheadLog] files. Calling this
     * method also closes the associated [FileChannel]s.
     */
    override fun delete() {
        super.delete()
        this.wal?.delete()
    }

    /**
     * This function makes sure, that the [WriteAheadLog] exists for ever write action.
     *
     * @param action The action that should be executed with the local [WriteAheadLog].
     */
    private inline fun <R> withWAL(action: (WriteAheadLog) -> R) : R {
        check(this.fileChannel.isOpen) { "DiskManager for {${this.path}} was closed and cannot be used to access data." }
        if (this.wal == null) {
            this.wal = WriteAheadLog(this.path.parent.resolve("${this.path.fileName}.wal"), this.lockTimeout, this.pages)
            this.header.isConsistent = false
            this.header.flush()
        }
        return action(this.wal!!)
    }
}