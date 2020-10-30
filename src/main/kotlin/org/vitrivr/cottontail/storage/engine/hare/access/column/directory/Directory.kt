package org.vitrivr.cottontail.storage.engine.hare.access.column.directory

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.storage.engine.hare.Address
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.access.TupleIdOutOfBoundException
import org.vitrivr.cottontail.storage.engine.hare.access.column.variable.HeaderPageView
import org.vitrivr.cottontail.storage.engine.hare.access.column.variable.VariableHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.basics.PageRef
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.views.Flags

/**
 * A [Directory] is a data structure that maps [TupleId]s to a pair of [Flags] and [Address]es.
 *
 * In its current form, it is basically a doubly linked list of [DirectoryPageView]s on which [TupleId]s
 * are arranged in increasing order. That structure makes it slow for random access to [TupleId]s, however,
 * in combination with cursors, data access through a [Directory] can be reasonably fast.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class Directory(internal val file: VariableHareColumnFile<*>, internal val bufferPool: BufferPool) {

    /** The [PageId] this [Directory] is currently pointing to. */
    @Volatile
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
        /* Seek to tupleId. */
        val directoryView = this.seek(tupleId)
        val ret = directoryView.getFlags(tupleId)
        (directoryView.page as PageRef).release() /* This is safe! */

        /* Return flags. */
        return ret
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
        /* Seek to tupleId. */
        val directoryView = this.seek(tupleId)
        val ret = directoryView.getAddress(tupleId)
        (directoryView.page as PageRef).release() /* This is safe! */

        /* Return addresss. */
        return ret
    }

    /**
     * Seeks the given [TupleId] and returns its [Flags] and [Address], if that [TupleId] was found.
     *
     * @param tupleId The [TupleId] to look for.
     * @return A [Pair] of [Flags] and [Address] for the [TupleId]
     *
     * @throws TupleIdOutOfBoundException If [TupleId] doesn't exist in [VariableHareColumnFile]
     */
    fun flagsAndAddress(tupleId: TupleId): Pair<Flags, Address> {
        /* Seek to tupleId. */
        val directoryView = this.seek(tupleId)
        val ret = Pair(directoryView.getFlags(tupleId), directoryView.getAddress(tupleId))
        (directoryView.page as PageRef).release() /* This is safe! */

        /* Return return both. */
        return ret
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
        val headerView = HeaderPageView(headerPage).validate()

        /* Prepare new directory page. */
        val directoryPage = this.bufferPool.get(headerView.lastDirectoryPageId, Priority.DEFAULT)
        val directoryView = DirectoryPageView(directoryPage).validate()
        val ret = if (directoryView.full) {
            /* Allocate directory page. */
            val newDirectoryPage = this.bufferPool.get(this.bufferPool.append(), Priority.LOW)
            DirectoryPageView.initialize(newDirectoryPage, this.pageId, directoryView.lastTupleId + 1)
            val newDirectoryView = DirectoryPageView(newDirectoryPage).validate()

            /* Update previous directory page. */
            directoryView.nextPageId = newDirectoryPage.id

            /* Initialize new directory page and allocate  tuple Id. */
            val tupleId = newDirectoryView.allocate(flags, address)

            /* Update this page and the header. */
            directoryView.nextPageId = newDirectoryPage.id
            headerView.lastDirectoryPageId = newDirectoryPage.id

            /* Release new page. */
            newDirectoryPage.release()

            tupleId
        } else {
            directoryView.allocate(flags, address)
        }

        /* Release header and directory page. */
        directoryPage.release()
        headerPage.release()
        return ret
    }

    /**
     * This is an internal method used to seek a specific [TupleId] within the [Directory]. Since
     * this method potentially influences the internal state of the [Directory], only one thread can
     * access this method at a time.
     *
     * <strong>Important:</strong> The method returns a [DirectoryPageView] of a retained [PageRef].
     * It is up to the caller to release that [PageRef].
     *
     * @param tupleId [TupleId] to seek.
     * @return [DirectoryPageView] reference of the [TupleId]
     */
    @Synchronized
    private fun seek(tupleId: TupleId): DirectoryPageView {
        var directoryPage = this.bufferPool.get(this.pageId, Priority.DEFAULT)
        var directoryView = DirectoryPageView(directoryPage)

        while (true) {
            /* Check if current page contains TupleId. */
            if (tupleId in directoryView.firstTupleId..directoryView.lastTupleId) {
                break
            } else if (tupleId > directoryView.lastTupleId) {
                if (directoryView.nextPageId == DirectoryPageView.NO_REF) {
                    throw TupleIdOutOfBoundException("The tupleId $tupleId is out of bound for variable length HARE column (file: ${this.file.path}).")
                }
                this.pageId = directoryView.nextPageId
            } else {
                if (directoryView.nextPageId == DirectoryPageView.NO_REF) {
                    throw TupleIdOutOfBoundException("The tupleId $tupleId is out of bound for variable length HARE column (file: ${this.file.path}).")
                }
                this.pageId = directoryView.nextPageId
            }

            /* Load new directory page. */
            directoryPage.release()
            directoryPage = this.bufferPool.get(this.pageId, Priority.DEFAULT)
            directoryView = DirectoryPageView(directoryPage)
        }
        return directoryView
    }
}