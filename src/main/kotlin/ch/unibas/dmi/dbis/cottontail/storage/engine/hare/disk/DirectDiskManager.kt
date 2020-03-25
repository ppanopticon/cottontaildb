package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.DataCorruptionException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.read
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import java.nio.ByteBuffer
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
 * @version 1.2
 * @author Ralph Gasser
 */
class DirectDiskManager(path: Path, lockTimeout: Long = 5000, private val preAllocatePages: Int = 32) : DiskManager(path, lockTimeout) {
    init {
        if (!this.header.isConsistent) {
            if (!this.validate()) {
                throw DataCorruptionException("CRC32C checksum mismatch (file: ${this.path}, expected:${this.calculateChecksum()}, found: ${this.header.checksum}}).")
            }
        }

        /* Updates sanity flag. */
        this.header.isConsistent = false
        this.header.flush()
    }

    /**
     * Fetches the data identified by the given [PageId] into the given [Page] object thereby
     * replacing the content of that [Page].
     *
     * @param id [PageId] to fetch data for.
     * @param page [Page] to fetch data into. Its content will be updated.
     */
    override fun read(id: PageId, page: Page) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to read data (file: ${this.path})." }
            this.fileChannel.read(page.data, this.pageIdToPosition(id))
        }
    }

    /**
     * Fetches the data starting from the given [PageId] into the given [Page] objects thereby replacing the content of those [Page]s.
     *
     * @param startId [PageId] to start fetching
     * @param pages [Page]s to fetch data into. Their content will be updated.
     */
    override fun read(startId: PageId, pages: Array<Page>) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to read data (file: ${this.path})." }
            val buffers = Array(pages.size) { pages[it].data }
            this.fileChannel.position(this.pageIdToPosition(startId))
            this.fileChannel.read(buffers)
        }
    }

    /**
     * Updates the [DataPage] in the HARE file managed by this [DirectDiskManager].
     *
     * @param id [PageId] of the [Page] that should be updated
     * @param data [Page] the data the [Page] should be updated with.
     */
    override fun update(id: PageId, page: Page) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to write data (file: ${this.path})." }
            this.fileChannel.write(page.data, this.pageIdToPosition(id))
        }
    }

    /**
     * Allocates new [Page] in the HARE file managed by this [DirectDiskManager].
     *
     * @param page [Page] to append. If empty, the allocated [Page] will be filled with zeros.
     */
    override fun allocate(page: Page?): PageId = this.closeLock.read {
        check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to write data (file: ${this.path})." }
        val newPageId = ++this.header.pages
        this.header.flush()
        if (page != null) {
            this.fileChannel.write(page.data, this.pageIdToPosition(newPageId))
        }

        /* Pre-allocate pages if file has reached limit. */
        if (this.fileChannel.size() == ((this.header.pages+1) shl this.header.pageShift)) {
            this.fileChannel.write(ByteBuffer.allocate(1), (this.header.pages + this.preAllocatePages) shl this.header.pageShift)
        }

        return newPageId
    }

    /**
     * Frees the given [Page] making space for new entries
     *
     * @param id The [PageId] that should be freed.
     */
    override fun free(id: PageId) = this.closeLock.read {
        check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to write data (file: ${this.path})." }
        TODO()
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
            this.header.flush()

            /* Close FileChannel and release file lock. */
            if (this.fileChannel.isOpen) {
                this.fileLock.release()
                this.fileChannel.close()
            }
        }
    }
}