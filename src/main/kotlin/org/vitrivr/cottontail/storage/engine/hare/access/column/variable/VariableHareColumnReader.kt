package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.EntryDeletedException
import org.vitrivr.cottontail.storage.engine.hare.access.column.directory.Directory
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnReader
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
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.shared
import java.util.concurrent.locks.StampedLock

/**
 * A [HareColumnReader] implementation for [FixedHareColumnFile]s.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class VariableHareColumnReader<T : Value>(val file: VariableHareColumnFile<T>, private val directory: Directory) : HareColumnReader<T> {

    /** The [Serializer] used to read data through this [FixedHareColumnReader]. */
    private val serializer: Serializer<T> = this.file.columnType.serializer(this.file.logicalSize)

    /** Flag indicating whether this [VariableHareColumnReader] has been closed.  */
    var closed: Boolean = false
        private set

    /** The [BufferPool] instance used by this [VariableHareColumnReader] is always shared with the [Directory]. */
    private val bufferPool: BufferPool = this.directory.bufferPool

    /** Flag indicating whether this [VariableHareColumnWriter] is open.  */
    @Volatile
    override var isOpen: Boolean = true
        private set

    /** A [StampedLock] that mediates access to methods of this [VariableHareColumnWriter]. */
    private val localLock = StampedLock()

    /** Obtains a lock on the [FixedHareColumnFile]. */
    private val lockHandle = this.file.obtainLock()

    init {
        require(this.file.isOpen) { "VariableHareColumnFile has been closed (file = ${this.file.path})." }
        require(this.file.disk == this.bufferPool.disk) { "VariableHareColumnFile and provided BufferPool do not share the same HareDiskManager." }
    }

    /**
     * Returns the entry for the given [TupleId] if such an entry exists.
     *
     * @param tupleId The [TupleId] to retrieve the entry for.
     * @return Entry [T] for the given [TupleId].
     */
    override fun get(tupleId: TupleId): T? = this.localLock.shared {
        /* Obtain and check flags for entry. */
        val flagsAndAddress = this.directory.flagsAndAddress(tupleId)
        if (flagsAndAddress.first.isDeleted()) {
            throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be accessed.")
        }

        /* Obtain address for entry. */
        val slotPage = this.bufferPool.get(flagsAndAddress.second.toPageId(), Priority.LOW)

        /* Obtain slotted page and read it. */
        val slottedView = SlottedPageView(slotPage).validate()
        val offset = slottedView.offset(flagsAndAddress.second.toSlotId())
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
    override fun count(): Long = this.localLock.shared {
        val page = this.bufferPool.get(VariableHareColumnFile.ROOT_PAGE_ID, Priority.HIGH)
        val ret = HeaderPageView(page).validate().count
        page.release()
        return ret
    }

    /**
     * Returns the maximum [TupleId] for the [HareColumnFile] backing this [HareColumnReader].
     *
     * @return The maximum [TupleId].
     */
    override fun maxTupleId(): TupleId = this.localLock.shared {
        val page = this.bufferPool.get(VariableHareColumnFile.ROOT_PAGE_ID, Priority.HIGH)
        val ret = HeaderPageView(page).validate().maxTupleId
        page.release()
        return ret
    }

    /**
     * Returns a boolean indicating whether the entry for the given [TupleId] is null.
     *
     * @param tupleId The [TupleId] to check.
     * @return true if the entry for the given [TupleId] is null and false otherwise.
     */
    override fun isNull(tupleId: TupleId): Boolean = this.localLock.shared {
        this.directory.flags(tupleId).isNull()
    }

    /**
     * Returns a boolean indicating whether the entry  for the given [TupleId] has been deleted.
     *
     * @return true if the entry for the given [TupleId] has been deleted and false otherwise.
     */
    override fun isDeleted(tupleId: TupleId): Boolean = this.localLock.shared {
        this.directory.flags(tupleId).isDeleted()
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