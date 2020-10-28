package org.vitrivr.cottontail.storage.engine.hare.disk.wal

import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
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
import kotlin.math.max

/**
 * The [WALHareDiskManager] facilitates reading and writing of [Page]s from/to the underlying disk storage. Only one
 * [HareDiskManager] can be opened per HARE file and it acquires an exclusive [FileLock] once created.
 *
 * As opposed to other [HareDiskManager] implementations, the [WALHareDiskManager] uses a write-ahead log (WAL) to make changes
 * to the underlying file. Upon commit or rollback, changes to the WAL are atomically transferred to the actual file. This
 * makes this implementation slower but offers some reliability circumstances that involve system crashes.
 *
 * @see HareDiskManager
 *
 * @version 1.3.1
 * @author Ralph Gasser
 */
class WALHareDiskManager(path: Path, lockTimeout: Long = 5000, private val preAllocatePages: Int = 32) : HareDiskManager(path, lockTimeout) {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(WALHareDiskManager::class.java)
    }

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
                LOGGER.warn("HARE file was found in a dirty state but no WAL file could be found (file: ${this.path.fileName}). Validating file...")
                if (!this.validate()) {
                    throw DataCorruptionException("CRC32C checksum mismatch (file: ${this.path}, expected:${this.calculateChecksum()}, found: ${this.header.checksum}}).")
                }
            }
        }
    }

    /**
     * Fetches the data identified by the given [PageId] into the given [HarePage] object thereby replacing the content
     * of that [HarePage]. [WALHareDiskManager]s always read directly from the underlying file. Thus, uncommitted changes to
     * the file are invisible.
     *
     * @param pageId [PageId] to fetch data for.
     * @param page [Page] to fetch data into. Its content will be updated.
     */
    override fun read(pageId: PageId, page: HarePage) {
        this.closeLock.shared {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to access data (file: ${this.path})." }
            page.lock.exclusive {
                this.fileChannel.read(page.buffer, this.pageIdToOffset(pageId))
                page.buffer.clear()
            }
        }
    }

    /**
     * Fetches the data starting from the given [PageId] into the given [HarePage] objects thereby replacing the content
     * of those [HarePage]s. [WALHareDiskManager]s always read directly from the underlying file. Thus, uncommitted changes to
     * the file are invisible.
     *
     * @param pageId [PageId] to start fetching
     * @param pages [HarePage]s to fetch data into. Their content will be updated.
     */
    override fun read(pageId: PageId, pages: Array<HarePage>) {
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
     * Updates the page  with the given [PageId] with the content in the [HarePage].
     *
     * This change will be written to the [WriteAheadLog].
     *
     * @param pageId [PageId] of the [Page] that should be updated
     * @param page [HarePage] the data the [Page] should be updated with.
     */
    override fun update(pageId: PageId, page: HarePage) = createOrUseSharedWAL {
        it.update(pageId, page)
    }

    /**
     * Allocates new [HarePage]s in the HARE page file managed by this [DirectHareDiskManager].
     *
     * The method will first try to return a [PageId] from the [LongStack] for free [PageId]s,
     * if that [LongStack] has run empty, then new pages are physically allocated and the file
     * will grow by the number of pages specified in [DirectHareDiskManager.preAllocatePages].
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
                    val newPageId = this.freePageStack.pop()
                    require(newPageId == entry.pageId) { "Failed to commit. The reused page ID $newPageId does not match the expected page ID ${entry.pageId} (file: ${this.path}, pages: ${this.pages})." }

                    /* Increment allocated pages count. */
                    this.header.allocatedPages += 1
                    this.header.maximumPageId = max(newPageId, this.header.maximumPageId)

                    /* Write changes to disk. */
                    this.header.write(this.fileChannel, OFFSET_HEADER)
                    this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)
                }
                WALAction.ALLOCATE_APPEND -> {
                    val newPageId = this.header.maximumPageId + 1
                    val preAllocatePageId = newPageId + this.preAllocatePages
                    require(newPageId == entry.pageId) { "Failed to commit. The new page ID $newPageId does not match the expected page ID ${entry.pageId} (file: ${this.path}, pages: ${this.pages})." }
                    for (pageId in preAllocatePageId..newPageId) {
                        this.freePageStack.offer(pageId)
                    }

                    /* Increment allocated pages count. */
                    this.header.allocatedPages += 1
                    this.header.maximumPageId = max(newPageId, this.header.maximumPageId)

                    /* Write changes to disk. */
                    this.header.write(this.fileChannel, OFFSET_HEADER)
                    this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)
                    this.fileChannel.write(EMPTY.clear(), (preAllocatePageId + 1) shl this.header.pageShift)
                }
                WALAction.FREE -> {
                    /* Add PageId to stack or increment number of dangling pages. */
                    if (!this.freePageStack.offer(entry.pageId)) {
                        this.header.danglingPages += 1
                    }

                    /* Decrement number of allocated pages. */
                    this.header.allocatedPages -= 1

                    /* Write changes to disk. */
                    val offset = pageIdToOffset(entry.pageId)
                    this.header.write(this.fileChannel, OFFSET_HEADER)
                    this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)
                    this.fileChannel.write(FREED.flip(), offset)
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
     * Deletes the HARE page file backing this [WALHareDiskManager] and associated [WriteAheadLog] files.
     *
     * Calling this method also closes the associated [FileChannel]s.
     */
    override fun delete() {
        super.delete()
        this.wal?.delete()
    }

    /**
     * Closes the [WriteAheadLog] associated with this [WALHareDiskManager].
     */
    override fun prepareClose() {
        /* Close WAL. */
        this.wal?.close()
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
                        /* Generate WriteAheadLogFile. */
                        this.wal = WriteAheadLog.create(this)

                        /* Update the file header to reflect start of WAL logging. */
                        this.header.isConsistent = false
                        this.header.write(this.fileChannel, OFFSET_HEADER)
                        this.fileChannel.force(false)
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