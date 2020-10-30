package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.addressFor
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer
import org.vitrivr.cottontail.storage.engine.hare.views.SlottedPageView
import org.vitrivr.cottontail.storage.engine.hare.views.VARIABLE_FLAGS_MASK_NULL
import java.lang.Long.max

/**
 * A [HareColumnWriter] implementation for [VariableHareColumnFile]s. This implementation is not thread safe!
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class VariableHareColumnWriter<T: Value> (val file: VariableHareColumnFile<T>): HareColumnWriter<T> {
    /** [BufferPool] for this [FixedHareColumnWriter] is always the one used by the [FixedHareColumnFile] (core pool). */
    private val bufferPool = this.file.bufferPool

    /** The [Serializer] used to read data through this [FixedHareColumnReader]. */
    private val serializer: Serializer<T> = this.file.columnDef.serializer

    /** The shared [HeaderPageView] instance for this [VariableHareColumnWriter].  */
    private val headerView = HeaderPageView()

    /** The shared [SlottedPageView] instance for this [VariableHareColumnWriter].  */
    private val slottedView = SlottedPageView()

    /** The shared [Directory] instance for this [VariableHareColumnWriter].  */
    private val directory = Directory(this.file)

    /** Flag indicating whether this [VariableHareColumnWriter] has been closed.  */
    var closed: Boolean = false
        private set

    override fun update(tupleId: TupleId, value: T?) {
        TODO("Not yet implemented")
    }

    override fun compareAndUpdate(tupleId: TupleId, expectedValue: T?, newValue: T?): Boolean {
        TODO("Not yet implemented")
    }

    override fun delete(tupleId: TupleId): T? {
        TODO("Not yet implemented")
    }

    /**
     * Appends a new value to the [VariableHareColumnFile].
     *
     * @param value The [T] to append. May be null.
     * @return The [TupleId] of the appended [Value].
     */
    override fun append(value: T?): TupleId {
        /** Try to allocate data on allocation page. */
        val allocationSize = if (value == null) {
            2
        } else {
            this.serializer.physicalSize
        }

        /** Get header page. */
        val headerPage = this.bufferPool.get(VariableHareColumnFile.ROOT_PAGE_ID)
        this.headerView.wrap(headerPage)

        /* Get allocation page. */
        var allocationPage = this.bufferPool.get(headerView.allocationPageId, Priority.LOW)
        this.slottedView.wrap(allocationPage)
        var slotId = this.slottedView.allocate(allocationSize) // TODO: Make dynamic

        /** If allocation page is full, create new one and store data there */
        if (slotId == null) {
            allocationPage.release()
            var allocationPageId = max(this.headerView.allocationPageId, this.headerView.lastDirectoryPageId) + 1L
            if (allocationPageId >= this.bufferPool.totalPages) {
                allocationPageId = this.bufferPool.append()
            }
            allocationPage = this.bufferPool.get(allocationPageId)
            SlottedPageView.initialize(allocationPage)
            slotId = this.slottedView.wrap(allocationPage).allocate(allocationSize) ?: TODO("Data that does not fit a single page.")
            this.headerView.allocationPageId = allocationPageId
        }

        /** Generate address and tupleId. */
        val address = addressFor(this.headerView.allocationPageId, slotId)
        val flags = if (value == null) {
            (0 or VARIABLE_FLAGS_MASK_NULL)
        } else {
            0
        }
        val newTupleId = this.directory.append(flags, address)
        this.headerView.maxTupleId = max(newTupleId, this.headerView.maxTupleId)

        /* Write data and release pages. */
        if (value != null) {
            this.serializer.serialize(allocationPage, this.slottedView.offset(slotId), value)
        }
        allocationPage.release()
        headerPage.release()

       return newTupleId
    }

    /**
     * Closes this [VariableHareColumnWriter].
     */
    override fun close() {
        if (!this.closed) {
            this.closed = true
        }
    }
}