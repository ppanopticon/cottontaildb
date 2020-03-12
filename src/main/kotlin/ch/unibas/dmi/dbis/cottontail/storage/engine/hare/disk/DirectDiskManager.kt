package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import java.nio.channels.FileLock
import java.nio.file.Path

/**
 * The [DirectDiskManager] facilitates reading and writing of [Page]s from/to the underlying disk storage. Only one
 * [DirectDiskManager] can be opened per HARE file and it acquires an exclusive [FileLock] once created.
 *
 * As opposed to other [DiskManager] implementations, the [DirectDiskManager] persistently writes all changes directly
 * to the underlying file. There is no semantic of committing and rolling back changes. This makes this implementation
 * rather fast but also unreliable in circumstances that involve system crashes.
 *
 * @see DiskManager
 *
 * @version 1.0
 * @author Ralph Gasser
 */
class DirectDiskManager(path: Path, lockTimeout: Long = 5000) : DiskManager(path, lockTimeout) {
    /**
     * Fetches the data identified by the given [PageId] into the given [Page] object thereby
     * replacing the content of that [Page].
     *
     * @param id [PageId] to fetch data for.
     * @param into [Page] to fetch data into. Its content will be updated.
     */
    override fun read(id: PageId, page: Page) {
        this.fileChannel.read(page.data, this.pageIdToPosition(id))
        page.id = id
        page.dirty = false
        page.data.rewind()
    }

    /**
     * Updates the [Page] in the HARE file managed by this [DirectDiskManager].
     *
     * @param page [Page] to update.
     */
    override fun update(page: Page) {
        this.fileChannel.write(page.data,this.pageIdToPosition(page.id))
        page.dirty = false
        page.data.rewind()
    }

    /**
     * Appends the [Page] to the HARE file managed by this [DirectDiskManager].
     *
     * @param page [Page] to append. Its [PageId] and flags will be updated.
     */
    override fun allocate(page: Page) {
        val pageId = ++this.header.pages
        this.header.flush()
        this.fileChannel.write(page.data, this.pageIdToPosition(pageId))
        page.id = pageId
        page.dirty = false
        page.data.rewind()
    }

    /**
     * Frees the [Page] identified by the given [PageId].
     *
     * @param pageId The [PageId] of the [Page] that should be freed.
     */
    override fun free(pageId: PageId) {}

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
}