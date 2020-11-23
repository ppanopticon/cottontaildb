package org.vitrivr.cottontail.storage.engine.hare.disk.direct

import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.TransactionId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.LongStack
import org.vitrivr.cottontail.utilities.extensions.read
import java.nio.channels.FileLock
import java.nio.file.Path
import kotlin.math.max

/**
 * The [DirectHareDiskManager] facilitates reading and writing of [Page]s from/to the underlying
 * HARE page file. Only one [HareDiskManager] can be opened per HARE file and it acquires an exclusive
 * [FileLock] once created.
 *
 * As opposed to other [HareDiskManager] implementations, the [DirectHareDiskManager] persistently
 * writes all changes directly to the underlying file. There is no semantic of committing and rolling
 * back changes. This makes this implementation fast but also unreliable in circumstances that involve
 * system crashes.
 *
 * @see HareDiskManager
 *
 * @version 1.3.3
 * @author Ralph Gasser
 */
class DirectHareDiskManager(path: Path, lockTimeout: Long = 5000, private val preAllocatePages: Int = 32) : HareDiskManager(path, lockTimeout) {
    companion object {
        private val LOGGER = LoggerFactory.getLogger(DirectHareDiskManager::class.java)
    }

    /**
     * Fetches the data identified by the given [PageId] into the given [Page] object thereby
     * replacing the content of that [Page].
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     * @param pageId [PageId] to fetch data for.
     * @param page [Page] to fetch data into. Its content will be updated.
     */
    override fun read(tid: TransactionId, pageId: PageId, page: HarePage) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to read data (file: ${this.path})." }
            page.read(this.fileChannel, this.pageIdToOffset(pageId))
        }
    }

    /**
     * Fetches the data starting from the given [PageId] into the given [Page] objects thereby replacing the content of those [Page]s.
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     * @param pageId [PageId] to start fetching
     * @param pages [HarePage]s to fetch data into. Their content will be updated.
     */
    override fun read(tid: TransactionId, pageId: PageId, pages: Array<HarePage>) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to read data (file: ${this.path})." }
            val locks = Array(pages.size) { pages[it].lock.writeLock() }
            val buffers = Array(pages.size) { pages[it].buffer.clear() }
            this.fileChannel.position(this.pageIdToOffset(pageId))
            this.fileChannel.read(buffers)
            locks.indices.forEach { i ->
                buffers[i].clear()
                pages[i].lock.unlockWrite(locks[i])
            }
        }
    }

    /**
     * Updates the [HarePage] in the HARE file managed by this [DirectHareDiskManager].
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     * @param pageId [PageId] of the [Page] that should be updated
     * @param page [HarePage] the data the [Page] should be updated with.
     */
    override fun update(tid: TransactionId, pageId: PageId, page: HarePage) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to write data (file: ${this.path})." }

            /* Write all changes to disk. */
            page.write(this.fileChannel, this.pageIdToOffset(pageId))
        }
    }

    /**
     * Allocates new [HarePage]s in the HARE page file managed by this [DirectHareDiskManager].
     *
     * The method will first try to return a [PageId] from the [LongStack] for free [PageId]s,
     * if that [LongStack] has run empty, then new pages are physically allocated and the file
     * will grow by the number of pages specified in [DirectHareDiskManager.preAllocatePages].
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     * @return The [PageId] of the allocated [Page].
     */
    override fun allocate(tid: TransactionId): PageId {
        return this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to write data (file: ${this.path})." }

            /* Pre-allocate pages if LongStack is empty. */
            if (this.freePageStack.entries == 0) {
                val nextPageId = this.header.maximumPageId + 1
                val preAllocatePageId = nextPageId + this.preAllocatePages
                this.fileChannel.write(EMPTY.clear(), (preAllocatePageId + 1) shl this.header.pageShift)
                for (pageId in preAllocatePageId downTo nextPageId) {
                    this.freePageStack.offer(pageId)
                }
            }

            /* Allocate PageId and adjust header. */
            val newPageId = this.freePageStack.pop()
            this.header.allocatedPages += 1
            this.header.maximumPageId = max(this.header.maximumPageId, newPageId)

            /* Write all changes to disk. */
            this.header.write(this.fileChannel, OFFSET_HEADER)
            this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)

            /* Return ID of next free page. */
            newPageId
        }
    }

    /**
     * Frees the page with the given [PageId] making space for new entries. The implementation uses a 2-tiered approach:
     *
     * 1) [PageId]s are added to the [freePageStack] and marked for re-use.
     * 2) If the [freePageStack] is full, the [Page] is left dangling and can only be reclaimed through compaction.
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     * @param pageId The [PageId] of the page that should be freed.
     */
    override fun free(tid: TransactionId, pageId: PageId) {
        this.closeLock.read {
            /* Sanity checks. */
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to write data (file: ${this.path})." }
            require(!this.freePageStack.contains(pageId)) { "The given page ID $pageId has already been freed for this HARE page file (file: ${this.path}, pages: ${this.pages})." }

            /* Free page by truncating the file (last page only) or by adding page to free page stack. */
            val offset = this.pageIdToOffset(pageId)
            if (!this.freePageStack.offer(pageId)) {
                this.header.danglingPages += 1
            }

            /* Decrement number of allocated pages. */
            this.header.allocatedPages -= 1

            /* Write all changes to disk. */
            this.header.write(this.fileChannel, OFFSET_HEADER)
            this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)
            this.fileChannel.write(FREED.flip(), offset)
        }
    }

    /**
     * Performs a pseudo-commit, by updating file checksum and setting consistency flag to true.
     */
    override fun commit(tid: TransactionId) { /* No Op. */
    }

    /**
     * Rollbacks are not supported by [DirectHareDiskManager]
     */
    override fun rollback(tid: TransactionId) { /* No Op. */
    }

    /**
     * Performs some sanity check before opening the file.
     */
    override fun prepareOpen() {
        if (!this.header.properlyClosed) {
            LOGGER.warn("HARE page file was not properly closed (file: ${this.path.fileName}). Validating file...")
            if (!this.validate()) {
                throw DataCorruptionException("HARE page file CRC32 checksum mismatch (file: ${this.path}, expected: ${this.calculateChecksum()}, found: ${this.header.checksum}}).")
            }
        }

        /* Updates sanity flag in header. */
        this.header.checksum = this.calculateChecksum()
        this.header.properlyClosed = false
        this.header.isDirty = false
        this.header.write(this.fileChannel, OFFSET_HEADER)
    }

    /**
     * Sets the properly closed flag to true and flushes the header.
     */
    override fun prepareClose() {
        this.header.properlyClosed = true
        this.header.write(this.fileChannel, OFFSET_HEADER)
    }
}