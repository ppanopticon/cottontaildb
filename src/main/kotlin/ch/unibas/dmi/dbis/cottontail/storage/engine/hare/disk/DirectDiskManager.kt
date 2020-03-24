package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.DataCorruptionException
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.read
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import java.nio.channels.FileLock
import java.nio.file.Path
import kotlin.concurrent.read
import kotlin.concurrent.write

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
 * @version 1.1
 * @author Ralph Gasser
 */
class DirectDiskManager(path: Path, lockTimeout: Long = 5000) : DiskManager(path, lockTimeout) {
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
            this.fileChannel.read(page.data.rewind(), this.pageIdToPosition(id))
        }
    }

    /**
     * Updates the [Page] in the HARE file managed by this [DirectDiskManager].
     *
     * @param id [PageId] of the [Page] that should be updated
     * @param page [Page] the data the [Page] should be updated with.
     */
    override fun update(id: PageId, page: Page) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "FileChannel for this HARE page file was closed and cannot be used to write data (file: ${this.path})." }
            this.fileChannel.write(page.data.rewind(), this.pageIdToPosition(id))
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
        this.fileChannel.write(page?.data?.rewind() ?: Page.EMPTY.data.rewind(), this.pageIdToPosition(newPageId))
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