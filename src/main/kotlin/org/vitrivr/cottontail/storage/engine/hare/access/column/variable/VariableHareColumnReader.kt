package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.EntryDeletedException
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnReader
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.HeaderPageView
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnReader
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer
import org.vitrivr.cottontail.storage.engine.hare.toPageId
import org.vitrivr.cottontail.storage.engine.hare.toSlotId
import org.vitrivr.cottontail.storage.engine.hare.views.SlottedPageView
import org.vitrivr.cottontail.storage.engine.hare.views.isDeleted
import org.vitrivr.cottontail.storage.engine.hare.views.isNull

/**
 * A [HareColumnReader] implementation for [FixedHareColumnFile]s. This implementation is not thread safe!
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class VariableHareColumnReader<T: Value>(val file: VariableHareColumnFile<T>, val directory: Directory): HareColumnReader<T> {
    /** [BufferPool] for this [FixedHareColumnWriter] is always the one used by the [FixedHareColumnWriter] (core pool). */
    private val bufferPool = this.file.bufferPool

    /** The [Serializer] used to read data through this [FixedHareColumnReader]. */
    private val serializer: Serializer<T> = this.file.columnDef.serializer

    /** The shared [HeaderPageView] instance for this [VariableHareColumnReader].  */
    private val headerView = org.vitrivr.cottontail.storage.engine.hare.access.column.variable.HeaderPageView()

    /** The shared [SlottedPageView] instance for this [VariableHareColumnReader].  */
    private val slottedView = SlottedPageView()

    /** Flag indicating whether this [VariableHareColumnReader] has been closed.  */
    var closed: Boolean = false
        private set

    /**
     * Returns the entry for the given [TupleId] if such an entry exists.
     *
     * @param tupleId The [TupleId] to retrieve the entry for.
     * @return Entry [T] for the given [TupleId].
     */
    override fun get(tupleId: TupleId): T? {
        /* Obtain and check flags for entry. */
        val flags = this.directory.flags(tupleId)
        if (flags.isDeleted()) {
            throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be accessed.")
        }

        /* Obtain address for entry. */
        val address = this.directory.address(tupleId)
        val slotPage = this.bufferPool.get(address.toPageId(), Priority.LOW)

        /* Obtain slotted page and read it. */
        this.slottedView.wrap(slotPage)
        val offset = this.slottedView.offset(address.toSlotId())
        val value = this.serializer.deserialize(slotPage, offset)

        /* Release page and return value. */
        slotPage.release()
        return value
    }

    /**
     * Returns the number of entries for the [HareColumnFile] backing this [HareColumnReader].
     *
     * @return Number of entries in this [HareColumnFile].
     */
    override fun count(): Long {
        val page = this.bufferPool.get(VariableHareColumnFile.ROOT_PAGE_ID, Priority.HIGH)
        val count = this.headerView.wrap(page).count
        page.release()
        return count
    }

    /**
     * Returns a boolean indicating whether the entry for the given [TupleId] is null.
     *
     * @param tupleId The [TupleId] to check.
     * @return true if the entry for the given [TupleId] is null and false otherwise.
     */
    override fun isNull(tupleId: TupleId): Boolean = this.directory.flags(tupleId).isNull()

    /**
     * Returns a boolean indicating whether the entry  for the given [TupleId] has been deleted.
     *
     * @return true if the entry for the given [TupleId] has been deleted and false otherwise.
     */
    override fun isDeleted(tupleId: TupleId): Boolean = this.directory.flags(tupleId).isDeleted()

    /**
     * Closes this [VariableHareColumnReader].
     */
    override fun close() {
        if (!this.closed) {
            this.closed = true
        }
    }
}