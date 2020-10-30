package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.storage.engine.hare.Address
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.access.TupleIdOutOfBoundException
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.views.DirectoryPageView
import org.vitrivr.cottontail.storage.engine.hare.views.Flags

/**
 * A [Directory] object. It can be used to lookup [TupleId]s in the directory of a [VariableHareColumnFile]
 * and to allocate new [TupleId]s. A newly created [Directory] starts at [VariableHareColumnFile.ROOT_DIRECTORY_PAGE_ID]
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class Directory(private val file: VariableHareColumnFile<*>) {

    /** The [PageId] this [Directory] is currently pointing to. */
    var pageId = VariableHareColumnFile.ROOT_DIRECTORY_PAGE_ID
        private set

    /** The [BufferPool] used by this [Directory]. */
    private val bufferPool: BufferPool = this.file.bufferPool

    /** The [HeaderPageView] used by this [Directory].  */
    private val headerView = HeaderPageView()

    /** The [DirectoryPageView] used by this [Directory].  */
    private val directoryView = DirectoryPageView()

    /**
     * Seeks the given [TupleId] and returns its [Address], if that [TupleId] was found.
     *
     * @param tupleId The [TupleId] to look for.
     * @return True, if [TupleId] was found. False otherwise.
     *
     * @throws TupleIdOutOfBoundException If [TupleId] doesn't exist in [VariableHareColumnFile]
     */
    fun flags(tupleId: TupleId): Flags {
        /* Fetch header page. */
        val headerPage = this.bufferPool.get(VariableHareColumnFile.ROOT_PAGE_ID, Priority.HIGH)
        this.headerView.wrap(headerPage)

        while (true) {
            val page = this.bufferPool.get(this.pageId, Priority.DEFAULT)
            this.directoryView.wrap(page)
            if (tupleId in directoryView.firstTupleId..directoryView.lastTupleId) {
                return this.directoryView.getFlags(tupleId)
            } else if (tupleId > this.directoryView.lastTupleId) {
                if (this.directoryView.nextPageId == DirectoryPageView.NO_REF) {
                    throw TupleIdOutOfBoundException("The tupleId $tupleId is out of bound for variable length HARE column (file: ${this.file.path}).")
                }
                this.pageId = this.directoryView.nextPageId
            } else {
                if (this.directoryView.nextPageId == DirectoryPageView.NO_REF) {
                    throw TupleIdOutOfBoundException("The tupleId $tupleId is out of bound for variable length HARE column (file: ${this.file.path}).")
                }
                this.pageId = this.directoryView.nextPageId
            }
        }
    }

    /**
     * Seeks the given [TupleId] and returns its [Flags], if that [TupleId] was found.
     *
     * @param tupleId The [TupleId] to look for.
     * @return [Flags] for the [TupleId]
     *
     * @throws TupleIdOutOfBoundException If [TupleId] doesn't exist in [VariableHareColumnFile]
     */
    fun address(tupleId: TupleId): Address {
        /* Fetch header page. */
        val headerPage = this.bufferPool.get(VariableHareColumnFile.ROOT_PAGE_ID, Priority.HIGH)
        this.headerView.wrap(headerPage)

        while (true) {
            val page = this.bufferPool.get(this.pageId, Priority.DEFAULT)
            try {
                if (tupleId in directoryView.firstTupleId..directoryView.lastTupleId) {
                    return this.directoryView.getAddress(tupleId)
                } else if (tupleId > this.directoryView.lastTupleId) {
                    if (this.directoryView.nextPageId == DirectoryPageView.NO_REF) {
                        throw TupleIdOutOfBoundException("The tupleId $tupleId is out of bound for variable length HARE column (file: ${this.file.path}).")
                    }
                    this.pageId = this.directoryView.nextPageId
                } else {
                    if (this.directoryView.nextPageId == DirectoryPageView.NO_REF) {
                        page.release()
                        throw TupleIdOutOfBoundException("The tupleId $tupleId is out of bound for variable length HARE column (file: ${this.file.path}).")
                    }
                    this.pageId = this.directoryView.previousPageId
                }
            } finally {
                headerPage.release()
                page.release()
            }
        }
    }

    /**
     * Allocates a new [TupleId] in this [Directory].
     *
     * @param flags The [Flags] for the new [TupleId]
     * @param address The [Address] for the new [TupleId]
     * @return New [TupleId]
     */
    fun append(flags: Flags, address: Address): TupleId {
        /* Fetch header page. */
        val headerPage = this.bufferPool.get(VariableHareColumnFile.ROOT_PAGE_ID, Priority.HIGH)
        this.headerView.wrap(headerPage)

        /* Prepare new directory page. */
        val directoryPage = this.bufferPool.get(this.headerView.lastDirectoryPageId, Priority.DEFAULT)
        this.directoryView.wrap(directoryPage)
        val ret = if (this.directoryView.full) {
            /* Allocate directory page. */
            val newDirectoryPage = this.bufferPool.get(this.bufferPool.append(), Priority.LOW)

            /* Update previous directory page. */
            this.directoryView.nextPageId = newDirectoryPage.id

            /* Initialize new directory page and allocate  tuple Id. */
            DirectoryPageView.initialize(newDirectoryPage, this.pageId, this.directoryView.lastTupleId + 1)
            this.directoryView.wrap(newDirectoryPage)
            val tupleId = this.directoryView.allocate(flags, address)

            /* Update this page and the header. */
            this.directoryView.nextPageId = newDirectoryPage.id
            this.headerView.lastDirectoryPageId = newDirectoryPage.id

            /* Release new page. */
            newDirectoryPage.release()

            tupleId
        } else {
            this.directoryView.allocate(flags, address)
        }
        directoryPage.release()
        headerPage.release()
        return ret
    }
}