package org.vitrivr.cottontail.storage.engine.hare.disk.wal

import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.DataPage
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.LongStack
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.shared
import org.vitrivr.cottontail.utilities.extensions.write
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
 * @version 1.3.0
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
                this.wal = WriteAheadLog(this, this.lockTimeout)
            } else {
                /*
                 * This can happen if system crashes between flushing the flag and creating the WAL file. This should
                 * have no implication on data consistency, hence, we can perform a CRC32 checksum check and set the
                 * flag to true.
                 */
                throw DataCorruptionException("Ongoing transaction was detected but no WAL file found for HARE file ${this.path.fileName}.")
            }
        }
    }

    /**
     * Fetches the data identified by the given [PageId] into the given [DataPage] object thereby replacing the content
     * of that [DataPage]. [WALDiskManager]s always read directly from the underlying file. Thus, uncommitted changes to
     * the file are invisible.
     *
     * @param pageId [PageId] to fetch data for.
     * @param page [Page] to fetch data into. Its content will be updated.
     */
    override fun read(pageId: PageId, page: DataPage) {
        this.closeLock.shared {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to access data (file: ${this.path})." }
            page.lock.exclusive {
                this.fileChannel.read(page.buffer, this.pageIdToOffset(pageId))
                page.buffer.clear()
            }
        }
    }

    /**
     * Fetches the data starting from the given [PageId] into the given [DataPage] objects thereby replacing the content
     * of those [DataPage]s. [WALDiskManager]s always read directly from the underlying file. Thus, uncommitted changes to
     * the file are invisible.
     *
     * @param pageId [PageId] to start fetching
     * @param pages [DataPage]s to fetch data into. Their content will be updated.
     */
    override fun read(pageId: PageId, pages: Array<DataPage>) {
        this.closeLock.shared {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to access data (file: ${this.path})." }
            val locks = Array(pages.size) { pages[it].lock.writeLock() }
            val buffers = Array(pages.size) { pages[it].buffer }
            this.fileChannel.position(this.pageIdToOffset(pageId))
            this.fileChannel.read(buffers)
            locks.indices.forEach { i ->
                buffers[i].clear()
                pages[i].lock.unlockWrite(locks[i])
            }
        }
    }

    /**
     * Updates the page  with the given [PageId] with the content in the [DataPage].
     *
     * This change will be written to the [WriteAheadLog].
     *
     * @param pageId [PageId] of the [Page] that should be updated
     * @param page [DataPage] the data the [Page] should be updated with.
     */
    override fun update(pageId: PageId, page: DataPage) = createOrUseSharedWAL {
        it.update(pageId, page)
    }

    /**
     * Allocates new [DataPage]s in the HARE page file managed by this [DirectDiskManager].
     *
     * The method will first try to return a [PageId] from the [LongStack] for free [PageId]s,
     * if that [LongStack] has run empty, then new pages are physically allocated and the file
     * will grow by the number of pages specified in [DirectDiskManager.preAllocatePages].
     *
     * This change will be written to the [WriteAheadLog].
     *
     * @return The [PageId] of the allocated [Page].
     */
    override fun allocate(): PageId = createOrUseSharedWAL {
        it.allocate()
    }

    /**
     * Frees the page with the given [PageId] making space for new entries
     *
     * @param pageId The [PageId] of the page that should be freed.
     */
    override fun free(pageId: PageId) = createOrUseSharedWAL {
        require(pageId in 1L..this.header.allocatedPages) { "The given page ID $pageId is out of bounds for this HARE page file (file: ${this.path}, pages: ${this.pages})." }
        it.free(pageId)
    }

    /**
     * Performs a commit of all pending changes by replaying the [WriteAheadLog] file.
     */
    override fun commit(): Unit = useExclusiveWAL {
        val pageSizeLong = this.pageSize.toLong()
        it.replay { entry, channel ->
            when (entry.action) {
                WALAction.UPDATE -> {
                    this.fileChannel.transferFrom(channel, this.pageIdToOffset(entry.pageId), pageSizeLong)
                }
                WALAction.ALLOCATE_REUSE -> {
                    val reusePageId = this.freePageStack.pop()
                    require(reusePageId == entry.pageId) { "Failed to commit. The reused page ID $reusePageId does not match the expected page ID ${entry.pageId} (file: ${this.path}, pages: ${this.pages})." }

                    /* Write changes to disk. */
                    this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)
                }
                WALAction.ALLOCATE_APPEND -> {
                    val newPageId = (++this.header.allocatedPages)
                    val preAllocatePageId = newPageId + this.preAllocatePages
                    require(newPageId == entry.pageId) { "Failed to commit. The new page ID $newPageId does not match the expected page ID ${entry.pageId} (file: ${this.path}, pages: ${this.pages})." }
                    for (pageId in preAllocatePageId..newPageId) {
                        this.freePageStack.offer(pageId)
                    }

                    /* Write changes to disk. */
                    this.header.write(this.fileChannel, OFFSET_HEADER)
                    this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)
                    this.fileChannel.write(EMPTY.clear(), (preAllocatePageId + 1) shl this.header.pageShift)
                }
                WALAction.FREE_REUSE -> {
                    this.freePageStack.offer(entry.pageId)

                    /* Write changes to disk. */
                    this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)
                }
                WALAction.FREE_TRUNCATE -> {
                    this.header.allocatedPages -= 1

                    /* Write changes to disk. */
                    this.fileChannel.truncate((this.header.allocatedPages + 1) shl this.header.pageShift)
                    this.header.write(this.fileChannel, OFFSET_HEADER)
                }
            }
            true
        }

        /* Update file header and force all data to disk. */
        this.header.isConsistent = true
        this.header.checksum = this.calculateChecksum()
        this.header.write(this.fileChannel, OFFSET_HEADER)
        this.fileChannel.force(true)

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
        this.header.write(this.fileChannel, OFFSET_HEADER)
        this.fileChannel.force(true)

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
        this.closeLock.shared {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file {${this.path}} was closed and cannot be used to write data (file: ${this.path})." }
            this.writeAheadLock.read {
                synchronized(this) {
                    if (this.wal == null) {
                        /* Update the file header to reflect start of logging. */
                        this.header.isConsistent = false
                        this.header.write(this.fileChannel, OFFSET_HEADER)

                        /* Generate WriteAheadLogFile. */
                        this.wal = WriteAheadLog.create(this)
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
        this.closeLock.shared {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file {${this.path}} was closed and cannot be used to write data (file: ${this.path})." }
            this.writeAheadLock.write {
                if (this.wal != null) {
                    action(this.wal!!)
                }
            }
        }
    }
}