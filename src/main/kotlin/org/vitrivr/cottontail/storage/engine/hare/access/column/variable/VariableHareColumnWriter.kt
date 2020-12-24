package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.model.basics.TransactionId
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.column.directory.Directory
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.addressFor
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer
import org.vitrivr.cottontail.storage.engine.hare.views.SlottedPageView
import org.vitrivr.cottontail.storage.engine.hare.views.VARIABLE_FLAGS_MASK_NULL
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.shared
import java.lang.Long.max
import java.util.concurrent.locks.StampedLock

/**
 * A [HareColumnWriter] implementation for [VariableHareColumnFile]s.
 *
 * @author Ralph Gasser
 * @version 1.0.3
 */
class VariableHareColumnWriter<T : Value>(val file: VariableHareColumnFile<T>, private val directory: Directory) : HareColumnWriter<T> {
    /** The [TransactionId] this [VariableHareColumnWriter] is associated with. */
    override val tid: TransactionId
        get() = this.bufferPool.tid

    /** Flag indicating whether this [VariableHareColumnWriter] is open.  */
    @Volatile
    override var isOpen: Boolean = true
        private set

    /** The [Serializer] used to read data through this [VariableHareColumnFile]. */
    private val serializer: Serializer<T> = this.file.columnType.serializer(this.file.logicalSize)

    /** The [BufferPool] instance used by this [VariableHareColumnWriter] is always shared with the [Directory]. */
    private val bufferPool: BufferPool = this.directory.bufferPool


    /** A [StampedLock] that mediates access to methods of this [VariableHareColumnWriter]. */
    private val localLock = StampedLock()

    /** Obtains a lock on the [FixedHareColumnFile]. */
    private val lockHandle = this.file.obtainLock()

    init {
        require(this.file.isOpen) { "VariableHareColumnFile has been closed (file = ${this.file.path})." }
        require(this.file.disk == this.bufferPool.disk) { "VariableHareColumnFile and provided BufferPool do not share the same HareDiskManager." }
    }

    override fun update(tupleId: TupleId, value: T?) = this.localLock.shared {
        TODO("Not yet implemented")
    }

    override fun compareAndUpdate(tupleId: TupleId, expectedValue: T?, newValue: T?): Boolean = this.localLock.shared {
        TODO("Not yet implemented")
    }

    override fun delete(tupleId: TupleId): T? = this.localLock.shared {
        TODO("Not yet implemented")
    }

    /**
     * Appends a new value to the [VariableHareColumnFile].
     *
     * @param value The [T] to append. May be null.
     * @return The [TupleId] of the appended [Value].
     */
    override fun append(value: T?): TupleId = this.localLock.shared {
        /** Try to allocate data on allocation page. */
        val allocationSize = if (value == null) {
            2
        } else {
            this.serializer.physicalSize
        }

        /** Get header page. */
        val headerPage = this.bufferPool.get(VariableHareColumnFile.ROOT_PAGE_ID)
        val headerView = HeaderPageView(headerPage).validate()

        /* Get allocation page. */
        var allocationPage = this.bufferPool.get(headerView.allocationPageId, Priority.LOW)
        var allocationPageView = SlottedPageView(allocationPage).validate()
        var slotId = allocationPageView.allocate(allocationSize) // TODO: Make dynamic

        /** If allocation page is full, create new one and store data there */
        if (slotId == null) {
            allocationPage.release()
            headerView.allocationPageId = this.bufferPool.append()
            allocationPage = this.bufferPool.get(headerView.allocationPageId)
            SlottedPageView.initialize(allocationPage)
            allocationPageView = SlottedPageView(allocationPage).validate()
            slotId = allocationPageView.allocate(allocationSize)
                    ?: TODO("Data that does not fit a single page.")
        }

        /** Generate address and tupleId. */
        val address = addressFor(headerView.allocationPageId, slotId)
        val flags = if (value == null) {
            (0 or VARIABLE_FLAGS_MASK_NULL)
        } else {
            0
        }
        val newTupleId = this.directory.append(flags, address)

        /* Update header. */
        headerView.maxTupleId = max(newTupleId, headerView.maxTupleId)
        headerView.count += 1

        /* Write data and release pages. */
        if (value != null) {
            this.serializer.serialize(allocationPage, allocationPageView.offset(slotId), value)
        }

        /* Release all pages. */
        allocationPage.release()
        headerPage.release()

        return newTupleId
    }

    /**
     * Commits all changes made through this [HareColumnWriter].
     */
    override fun commit() = this.localLock.exclusive {
        this.bufferPool.flush()
        this.file.disk.commit(this.tid)
    }

    /**
     * Performs a rollback on all changes made through this [HareColumnWriter].
     */
    override fun rollback() = this.localLock.exclusive {
        this.bufferPool.synchronize()
        this.file.disk.rollback(this.tid)
    }

    /**
     * Closes this [VariableHareColumnWriter].
     */
    override fun close() = this.localLock.exclusive {
        if (this.isOpen) {
            this.isOpen = false
            this.file.releaseLock(this.lockHandle)
        }
    }
}