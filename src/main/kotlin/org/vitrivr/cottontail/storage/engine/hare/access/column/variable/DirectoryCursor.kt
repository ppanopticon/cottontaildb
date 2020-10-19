package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.storage.engine.hare.Address
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile.FixedHareCursor
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.views.DirectoryPageView
import org.vitrivr.cottontail.storage.engine.hare.views.Flags
import java.lang.Long

/**
 * A [DirectoryCursor] object. It points to a [DirectoryPageView] and can be used to traverse a
 * [VariableHareColumnFile]'s directory entries.
 *
 * A newly created [DirectoryCursor] starts at [VariableHareColumnFile.ROOT_DIRECTORY_PAGE_ID]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class DirectoryCursor(private val bufferPool: BufferPool) : AutoCloseable {
    /** The [HeaderPageView] used by this [DirectoryCursor]. */
    val headerView = HeaderPageView()

    /** The [DirectoryPageView] used by this [DirectoryCursor]. */
    private val directoryView = DirectoryPageView()

    /** The [PageId] this [DirectoryCursor] is currently pointing to. */
    var pageId = VariableHareColumnFile.ROOT_DIRECTORY_PAGE_ID
        private set

    /** The first [TupleId] this [DirectoryCursor] is currently pointing to. */
    val firstTupleId: TupleId
        get() = this.directoryView.firstTupleId

    /** The last [TupleId] this [DirectoryCursor] is currently pointing to. */
    val lastTupleId: TupleId
        get() = this.directoryView.lastTupleId

    /** Internal flag used to indicate, that this [FixedHareCursor] was closed. */
    @Volatile
    private var closed: Boolean = false

    init {
        /* Navigates to ROOT_DIRECTORY_PAGE_ID. */
        this.first()
        this.headerView.wrap(this.bufferPool.get(VariableHareColumnFile.ROOT_PAGE_ID))
    }

    /**
     * Returns true, if this [DirectoryCursor] is pointing to the [DirectoryPageView] that
     * contains the given [TupleId].
     *
     * @param tupleId The [TupleId] to check.
     * @return True, if [TupleId] is contained in current [DirectoryPageView]
     */
    fun has(tupleId: TupleId): Boolean = this.directoryView.has(tupleId)

    /**
     * Moves [DirectoryCursor] to the previous [DirectoryPageView].
     *
     * @return True if [DirectoryCursor] could be moved to the previous [DirectoryPageView]
     */
    fun previous(): Boolean = this.goTo(this.directoryView.previousPageId)

    /**
     * Moves [DirectoryCursor] to the next [DirectoryPageView].
     *
     * @return True if [DirectoryCursor] could be moved to the next [DirectoryPageView]
     */
    fun next(): Boolean = this.goTo(this.directoryView.nextPageId)

    /**
     * Moves [DirectoryCursor] to the first [DirectoryPageView].
     *
     * @return True if [DirectoryCursor] could be moved to the first [DirectoryPageView]
     */
    fun first(): Boolean = this.goTo(VariableHareColumnFile.ROOT_DIRECTORY_PAGE_ID)

    /**
     * Moves [DirectoryCursor] to the last [DirectoryPageView].
     *
     * @return True if [DirectoryCursor] could be moved to the next [DirectoryPageView]
     */
    fun last(): Boolean = this.goTo(this.headerView.lastDirectoryPageId)

    /**
     *
     */
    fun getFlags(tupleId: TupleId): Flags = this.directoryView.getFlags(tupleId)

    /**
     *
     */
    fun getAddress(tupleId: TupleId): Address = this.directoryView.getAddress(tupleId)

    /**
     * Allocates a new [TupleId] in this [DirectoryCursor]. The new [TupleId] is merely prepared but
     * not pointing to any location.
     */
    fun newTupleId(flags: Flags, address: Address): TupleId {
        /* Go to last page directory. */
        this.goTo(this.headerView.lastDirectoryPageId)

        /* Prepare new directory page. */
        if (this.directoryView.full) {
            val newDirectoryPageId = Long.max(this.headerView.allocationPageId, this.headerView.lastDirectoryPageId) + 1L
            var tupleId = -1L
            if (newDirectoryPageId >= this.bufferPool.totalPages) {
                val page = this.bufferPool.detach()
                val view = DirectoryPageView().initializeAndWrap(page, this.pageId, this.lastTupleId + 1)
                tupleId = view.allocate(flags, address)
                this.bufferPool.append(page)
                page.release()
            } else {
                val page = this.bufferPool.get(newDirectoryPageId)
                val view = DirectoryPageView().initializeAndWrap(page, this.pageId, this.lastTupleId + 1)
                tupleId = view.allocate(flags, address)
                page.release()
            }

            /* Update this page and the header. */
            this.directoryView.nextPageId = newDirectoryPageId
            this.headerView.lastDirectoryPageId = newDirectoryPageId

            return tupleId
        } else {
            return this.directoryView.allocate(flags, address)
        }
    }

    /**
     * Moves [DirectoryCursor] to the given [PageId].
     *
     * @return True if [DirectoryCursor] could be moved to the given [PageId]
     */
    private fun goTo(pageId: PageId): Boolean {
        return if (pageId > DirectoryPageView.NO_REF) {
            val previous = this.directoryView.page
            if (previous is BufferPool.PageReference) {
                previous.release()
            }
            this.directoryView.wrap(this.bufferPool.get(pageId))
            true
        } else {
            false
        }
    }

    /**
     * Closes this [DirectoryCursor] and releases [Page]s.
     */
    override fun close() {
        if (!this.closed) {
            val previous = this.directoryView.page
            if (previous is BufferPool.PageReference) {
                previous.release()
            }
            val header = this.headerView.page
            if (header is BufferPool.PageReference) {
                header.release()
            }
            this.closed = true
        }
    }
}