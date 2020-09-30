package org.vitrivr.cottontail.storage.engine.hare.disk.wal

import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.disk.DataPage
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.locks.ReentrantLock
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
 * @version 1.2
 * @author Ralph Gasser
 */
class WALDiskManager(path: Path, lockTimeout: Long = 5000, private val preAllocatePages: Int = 32) : DiskManager(path, lockTimeout) {

    /** Reference to the [WriteAheadLog]. The [WriteAheadLog] is created whenever a write starts and removed on commit or rollback. */
    @Volatile
    private var wal: WriteAheadLog? = null

    /** A [ReentrantLock] to mediate access to [WriteAheadLog]. */
    private val writeAheadLock = StampedLock()

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

    override fun read(id: PageId, page: DataPage) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to access data (file: ${this.path})." }
            page.lock.exclusive {
                this.fileChannel.read(page._data, this.pageIdToPosition(id))
                page._data.clear()
            }
        }
    }

    override fun read(startId: PageId, pages: Array<DataPage>) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to access data (file: ${this.path})." }
            val locks = Array(pages.size) { pages[it].lock.writeLock() }
            val buffers = Array(pages.size) { pages[it]._data }
            this.fileChannel.position(this.pageIdToPosition(startId))
            this.fileChannel.read(buffers)
            locks.indices.forEach { i ->
                buffers[i].clear()
                pages[i].lock.unlockWrite(locks[i])
            }
        }
    }

    override fun update(id: PageId, page: DataPage) = createOrUseSharedWAL {
        require(id <= it.maxPageId && id >= 1) { "The given page ID $id is out of bounds for this HARE page file (file: ${this.path}, pages: ${this.pages})." }
        it.append(action = WALAction.UPDATE, id = id, page = page)
    }


    override fun allocate(page: DataPage?): PageId = createOrUseSharedWAL {
        it.append(action = WALAction.APPEND, page = page)
        it.maxPageId
    }

    override fun free(id: PageId) = createOrUseSharedWAL {
        require(id <= it.maxPageId && id >= 1) { "The given page ID $id is out of bounds for this HARE page file (file: ${this.path}, pages: ${this.pages})." }
        it.append(action = WALAction.FREE, id = id)
    }

    /**
     * Performs a commit of all pending changes by replaying the [WriteAheadLog] file.
     */
    override fun commit(): Unit = useExclusiveWAL {
        val pageSizeLong = this.pageSize.toLong()
        it.replay { action, id, b ->
            when (action) {
                WALAction.UPDATE ->{
                    this.fileChannel.transferFrom(b, this.pageIdToPosition(id), pageSizeLong)
                }
                WALAction.APPEND -> {
                    /* Default case; page has not been allocated yet */
                    if (id-1 == this.header.pages) {
                        this.header.pages += this.preAllocatePages
                        this.header.flush()
                        this.fileChannel.write(ByteBuffer.allocate(1), (this.header.pages + this.preAllocatePages) shl this.header.pageShift)
                    }
                    this.fileChannel.transferFrom(b, this.pageIdToPosition(id), pageSizeLong)
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
        it.close()
        it.delete()
        this.wal = null
    }

    /**
     * Performs a rollback of all pending changes by discarding the [WriteAheadLog] file.
     */
    override fun rollback() = useExclusiveWAL {
        /* Update file header. */
        this.header.isConsistent = true
        this.header.flush()

        /** Delete WAL. */
        it.close()
        it.delete()
        this.wal = null
    }

    /**
     * Deletes the HARE page file backing this [WALDiskManager] and associated [WriteAheadLog] files.
     *
     * Calling this method also closes the associated [FileChannel]s.
     */
    override fun delete() {
        super.delete()
        this.wal?.delete()
    }

    /**
     * Closes the HARE page file backing this [WALDiskManager] and associated [WriteAheadLog] files.
     */
    override fun close() = this.closeLock.write {
        if (this.isOpen) {
            /* Close WAL. */
            this.wal?.close()

            /* Close FileChannel and release file lock. */
            if (this.fileChannel.isOpen) {
                this.fileLock.release()
                this.fileChannel.close()
            }
        }
    }

    /**
     * This function acquires a lock on the [WriteAheadLog] entry then checks, if the [WriteAheadLog]
     * exists. If not, a new [WriteAheadLog] is created.
     *
     * @param action The action that should be executed with the local [WriteAheadLog].
     */
    private inline fun <R> createOrUseSharedWAL(action: (WriteAheadLog) -> R) : R {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file {${this.path}} was closed and cannot be used to write data (file: ${this.path})." }
            this.writeAheadLock.read {
                synchronized(this) {
                    if (this.wal == null) {
                        val walPath = this.path.parent.resolve("${this.path.fileName}.wal")
                        WriteAheadLog.create(walPath, this.pages, this.pageShift)
                        this.wal = WriteAheadLog(walPath, this.lockTimeout)
                        this.header.isConsistent = false
                        this.header.flush()
                    }
                }
                return action(this.wal!!)
            }
        }
    }

    /**
     * This function acquires a lock on the [WriteAheadLog] entry then checks, if the [WriteAheadLog]
     * exists. If so, the action will be executed, otherwise, nothing happens.
     *
     * @param action The action that should be executed with the local [WriteAheadLog].
     */
    private inline fun useExclusiveWAL(action: (WriteAheadLog) -> Unit) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file {${this.path}} was closed and cannot be used to write data (file: ${this.path})." }
            this.writeAheadLock.write {
                if (this.wal != null) {
                    action(this.wal!!)
                }
            }
        }
    }
}