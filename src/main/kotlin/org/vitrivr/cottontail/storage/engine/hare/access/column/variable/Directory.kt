package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.storage.engine.hare.Address
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.access.TupleIdOutOfBoundException
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.views.DirectoryPageView
import org.vitrivr.cottontail.storage.engine.hare.views.Flags
import java.lang.Long.max

/**
 * A [Directory] object. It can be used to lookup [TupleId]s in the directory of a [VariableHareColumnFile]
 * and to allocate new [TupleId]s. A newly created [Directory] starts at [VariableHareColumnFile.ROOT_DIRECTORY_PAGE_ID]
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class Directory(private val file: VariableHareColumnFile<*>) {

    /** The [BufferPool] used by this [Directory]. */
    val bufferPool: BufferPool = this.file.bufferPool

    /** The [PageId] this [Directory] is currently pointing to. */
    var pageId = VariableHareColumnFile.ROOT_DIRECTORY_PAGE_ID
        private set

    /**
     * Seeks the given [TupleId] and returns its [Address], if that [TupleId] was found.
     *
     * @param tupleId The [TupleId] to look for.
     * @return True, if [TupleId] was found. False otherwise.
     *
     * @throws TupleIdOutOfBoundException If [TupleId] doesn't exist in [VariableHareColumnFile]
     */
    fun flags(tupleId: TupleId): Flags {
        var pageId = this.pageId
        while (true) {
            val page = this.bufferPool.get(pageId, Priority.DEFAULT)
            val directoryView = DirectoryPageView().wrap(page)
            if (tupleId in directoryView.firstTupleId..directoryView.lastTupleId) {
                return directoryView.getFlags(tupleId)
            } else if (tupleId > directoryView.lastTupleId) {
                if (directoryView.nextPageId == DirectoryPageView.NO_REF) {
                    throw TupleIdOutOfBoundException("The tupleId $tupleId is out of bound for variable length HARE column (file: ${this.file.path}).")
                }
                pageId = directoryView.nextPageId
            } else {
                if (directoryView.nextPageId == DirectoryPageView.NO_REF) {
                    throw TupleIdOutOfBoundException("The tupleId $tupleId is out of bound for variable length HARE column (file: ${this.file.path}).")
                }
                pageId = directoryView.previousPageId
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
        var pageId = this.pageId
        while (true) {
            val page = this.bufferPool.get(pageId, Priority.DEFAULT)
            val directoryView = DirectoryPageView().wrap(page)
            if (tupleId in directoryView.firstTupleId..directoryView.lastTupleId) {
                return directoryView.getAddress(tupleId)
            } else if (tupleId > directoryView.lastTupleId) {
                if (directoryView.nextPageId == DirectoryPageView.NO_REF) {
                    throw TupleIdOutOfBoundException("The tupleId $tupleId is out of bound for variable length HARE column (file: ${this.file.path}).")
                }
                pageId = directoryView.nextPageId
            } else {
                if (directoryView.nextPageId == DirectoryPageView.NO_REF) {
                    throw TupleIdOutOfBoundException("The tupleId $tupleId is out of bound for variable length HARE column (file: ${this.file.path}).")
                }
                pageId = directoryView.previousPageId
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
        /* Go to last page directory. */
        val headerPage = this.bufferPool.get(VariableHareColumnFile.ROOT_PAGE_ID, Priority.HIGH)
        val headerView = HeaderPageView().wrap(headerPage)

        val directoryPage = this.bufferPool.get(headerView.lastDirectoryPageId, Priority.DEFAULT)
        val directoryView = DirectoryPageView().wrap(directoryPage)

        /* Prepare new directory page. */
        val ret = if (directoryView.full) {
            val newDirectoryPageId = max(headerView.allocationPageId, headerView.lastDirectoryPageId) + 1L
            var tupleId = -1L
            val newDirectoryPage = this.bufferPool.get(this.bufferPool.append(), Priority.LOW)
            val newDirectoryPageView = DirectoryPageView().initializeAndWrap(newDirectoryPage, this.pageId, directoryView.lastTupleId + 1)
            tupleId = newDirectoryPageView.allocate(flags, address)
            newDirectoryPage.release()

            /* Update this page and the header. */
            directoryView.nextPageId = newDirectoryPageId
            headerView.lastDirectoryPageId = newDirectoryPageId
            tupleId
        } else {
            directoryView.allocate(flags, address)
        }

        /** Free pages. */
        directoryPage.release()
        headerPage.release()

        return ret
    }
}