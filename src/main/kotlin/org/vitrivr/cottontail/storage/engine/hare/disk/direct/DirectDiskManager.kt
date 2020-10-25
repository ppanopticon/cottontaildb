package org.vitrivr.cottontail.storage.engine.hare.disk.direct

import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.DataPage
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.LongStack
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.channels.FileLock
import java.nio.file.Path

/**
 * The [DirectDiskManager] facilitates reading and writing of [Page]s from/to the underlying HARE page file. Only one
 * [DiskManager] can be opened per HARE file and it acquires an exclusive [FileLock] once created.
 *
 * As opposed to other [DiskManager] implementations, the [DirectDiskManager] persistently writes all changes directly
 * to the underlying file. There is no semantic of committing and rolling back changes. This makes this implementation
 * fast but also unreliable in circumstances that involve system crashes.
 *
 * @see DiskManager
 *
 * @version 1.3.0
 * @author Ralph Gasser
 */
class DirectDiskManager(path: Path, lockTimeout: Long = 5000, private val preAllocatePages: Int = 32) : DiskManager(path, lockTimeout) {

    init {
        if (!this.header.isConsistent) {
            if (!this.validate()) {
                throw DataCorruptionException("CRC32C checksum mismatch (file: ${this.path}, expected:${this.calculateChecksum()}, found: ${this.header.checksum}}).")
            }
        }

        /* Updates sanity flag in header. */
        this.header.isConsistent = false
        this.header.write(this.fileChannel, OFFSET_HEADER)
    }

    /**
     * Fetches the data identified by the given [PageId] into the given [Page] object thereby
     * replacing the content of that [Page].
     *
     * @param pageId [PageId] to fetch data for.
     * @param page [Page] to fetch data into. Its content will be updated.
     */
    override fun read(pageId: PageId, page: DataPage) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to read data (file: ${this.path})." }
            page.read(this.fileChannel, this.pageIdToOffset(pageId))
        }
    }

    /**
     * Fetches the data starting from the given [PageId] into the given [Page] objects thereby replacing the content of those [Page]s.
     *
     * @param pageId [PageId] to start fetching
     * @param pages [DataPage]s to fetch data into. Their content will be updated.
     */
    override fun read(pageId: PageId, pages: Array<DataPage>) {
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
     * Updates the [DataPage] in the HARE file managed by this [DirectDiskManager].
     *
     * @param pageId [PageId] of the [Page] that should be updated
     * @param page [DataPage] the data the [Page] should be updated with.
     */
    override fun update(pageId: PageId, page: DataPage) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to write data (file: ${this.path})." }
            page.write(this.fileChannel, this.pageIdToOffset(pageId))
        }
    }

    /**
     * Allocates new [DataPage]s in the HARE page file managed by this [DirectDiskManager].
     *
     * The method will first try to return a [PageId] from the [LongStack] for free [PageId]s,
     * if that [LongStack] has run empty, then new pages are physically allocated and the file
     * will grow by the number of pages specified in [DirectDiskManager.preAllocatePages].
     *
     * @return The [PageId] of the allocated [Page].
     */
    override fun allocate(): PageId = this.closeLock.read {
        check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to write data (file: ${this.path})." }

        /* Pre-allocate pages if LongStack is empty. */
        if (this.freePageStack.entries == 0) {
            val nextPageId = this.header.allocatedPages + 1
            val preAllocatePageId = nextPageId + this.preAllocatePages
            this.fileChannel.write(EMPTY.clear(), (preAllocatePageId + 1) shl this.header.pageShift)
            for (pageId in preAllocatePageId downTo nextPageId) {
                this.freePageStack.offer(pageId)
            }
        }

        /* Allocate PageId. */
        val newPageId = this.freePageStack.pop()
        this.header.allocatedPages += 1

        /* Flush Header and LongStack. */
        this.header.write(this.fileChannel, OFFSET_HEADER)
        this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)

        /* Return ID of next free page. */
        return newPageId
    }

    /**
     * Frees the page with the given [PageId] making space for new entries
     *
     * @param pageId The [PageId] of the page that should be freed.
     */
    override fun free(pageId: PageId) = this.closeLock.read {
        check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to write data (file: ${this.path})." }
        require(pageId in 1L..this.header.allocatedPages) { "The given page ID $pageId is out of bounds for this HARE page file (file: ${this.path}, pages: ${this.pages})." }

        /* Free page by truncating (last page only) or by adding page to free page stack. */
        if (pageId == this.header.allocatedPages) {
            this.header.allocatedPages -= 1
            this.fileChannel.truncate(this.fileChannel.size() - this.pageSize)
        } else if (!this.freePageStack.offer(pageId)) {
            TODO("Free page stack is full; file needs compaction!")
        }

        /* Flush Header and LongStack. */
        this.header.write(this.fileChannel, OFFSET_HEADER)
        this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)
        Unit
    }

    /**
     * Commits all changes made through this [DirectDiskManager].
     */
    override fun commit() {
        /* Does not have an effect. */
    }

    /**
     * Rolls back all changes made through this [DirectDiskManager].
     */
    override fun rollback() {
        /* Does not have an effect. */
    }

    /**
     * Closes this [DiskManager]. Will cause the [DiskManager.Header] to be finalized properly.
     */
    override fun close() = this.closeLock.write {
        if (this.isOpen) {
            /* Update consistency information in the header. */
            this.header.checksum = this.calculateChecksum()
            this.header.isConsistent = true
            this.header.write(this.fileChannel, OFFSET_HEADER)

            /* Close FileChannel and release file lock. */
            if (this.fileChannel.isOpen) {
                this.fileLock.release()
                this.fileChannel.close()
            }
        }
    }
}