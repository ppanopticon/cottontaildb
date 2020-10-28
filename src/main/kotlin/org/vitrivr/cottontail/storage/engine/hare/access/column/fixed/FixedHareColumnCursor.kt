package org.vitrivr.cottontail.storage.engine.hare.access.column.fixed

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareCursor
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.toPageId
import org.vitrivr.cottontail.storage.engine.hare.toSlotId

/**
 * A [HareCursor] implementation for [FixedHareColumnFile]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FixedHareColumnCursor<T: Value>(val file: FixedHareColumnFile<T>, range: LongRange? = null): HareCursor<T> {

    /** [BufferPool] for this [FixedHareColumnWriter] is always the one used by the [FixedHareColumnWriter] (core pool). */
    private val bufferPool = this.file.bufferPool

    /** The [TupleId] this [FixedHareColumnCursor] is currently pointing to. */
    override var tupleId: TupleId = HareCursor.CURSOR_BOF

    /** Minimum [TupleId] that can be accessed through this [FixedHareColumnCursor]. */
    override val start: TupleId

    /** Maximum [TupleId] that can be accessed through this [FixedHareColumnCursor]. */
    override val end: TupleId

    /** [start] and [end] are initialized once! Hence [FixedHareColumnCursor] won't reflect changes to the file.*/
    init {
        val page = this.bufferPool.get(FixedHareColumnFile.ROOT_PAGE_ID, Priority.HIGH)
        val header = HeaderPageView().wrap(page)
        if (range != null) {
            require(range.first >= 0L) { "Start tupleId must be greater or equal than zero."}
            require(range.last <= header.maxTupleId) { "End tupleId must be smaller or equal to to maximum tupleId for HARE file."}
            this.start = range.first
            this.end = range.last
        } else {
            this.start = 0L
            this.end = header.maxTupleId
        }
        page.release()
    }

    /**
     * Checks if this [FixedHareColumnCursor] has a next item and thereby moves the cursor to that position.
     *
     * @return True, if [FixedHareColumnCursor] has another entry, false otherwise.
     */
    override fun hasNext(): Boolean {
        if (this.tupleId == HareCursor.CURSOR_BOF) {
            this.tupleId = this.start
        } else {
            this.tupleId += 1
        }
        while (true) {
            if (this.tupleId > this.end) return false
            if (isValid(this.tupleId)) return true
            this.tupleId += 1
        }
    }

    /**
     * Returns next [TupleId].
     */
    override fun next(): TupleId = this.tupleId

    /**
     * Returns a boolean indicating whether the entry  for the given [TupleId] has been deleted.
     *
     * @return true if the entry at the current position of the [FixedHareCursor] has been deleted and false otherwise.
     */
    private fun isValid(tupleId: TupleId): Boolean {
        /* Calculate necessary offsets. */
        val address = this.file.toAddress(tupleId)
        val pageId = address.toPageId()
        val slotId = address.toSlotId()
        val entryOffset: Int = slotId * this.file.entrySize

        val page = this.bufferPool.get(pageId, Priority.DEFAULT)
        try {
            return (page.getInt(entryOffset) and FixedHareColumnFile.MASK_DELETED.inv()) > 0L
        } finally {
            page.release()
        }
    }
}
