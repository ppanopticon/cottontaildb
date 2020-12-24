package org.vitrivr.cottontail.storage.engine.hare.access.column.fixed

import org.vitrivr.cottontail.model.basics.TransactionId
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.EntryDeletedException
import org.vitrivr.cottontail.storage.engine.hare.access.NullValueNotAllowedException
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer
import org.vitrivr.cottontail.storage.engine.hare.toPageId
import org.vitrivr.cottontail.storage.engine.hare.toSlotId
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.shared

import java.lang.Long.max
import java.util.concurrent.locks.StampedLock

/**
 * A [HareColumnWriter] implementation for [FixedHareColumnFile]s.
 *
 * @author Ralph Gasser
 * @version 1.0.3
 */
class FixedHareColumnWriter<T : Value>(val file: FixedHareColumnFile<T>, private val bufferPool: BufferPool) : HareColumnWriter<T> {

    /** The [TransactionId] this [FixedHareColumnWriter] is associated with. */
    override val tid: TransactionId
        get() = this.bufferPool.tid

    /** Flag indicating whether this [FixedHareColumnWriter] is open.  */
    @Volatile
    override var isOpen: Boolean = true
        private set

    /** The [Serializer] used to read data through this [FixedHareColumnReader]. */
    private val serializer: Serializer<T> = this.file.columnType.serializer(this.file.logicalSize)

    /** A [StampedLock] that mediates access to methods of this [FixedHareColumnWriter]. */
    private val localLock = StampedLock()

    /** Obtains a lock on the [FixedHareColumnFile]. */
    private val lockHandle = this.file.obtainLock()

    init {
        require(this.file.isOpen) { "FixedHareColumnFile has been closed (file = ${this.file.path})." }
        require(this.file.disk == this.bufferPool.disk) { "FixedHareColumnFile and provided BufferPool do not share the same HareDiskManager." }
    }

    /**
     * Updates the entry for the given [TupleId].
     *
     * @param tupleId The [TupleId] to update the entry for.
     * @param value The new value [T] the updated entry should contain or null.
     */
    override fun update(tupleId: TupleId, value: T?) = this.localLock.shared {
        /* Check nullability constraint. */
        if (value == null && !this.file.nullable) {
            throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
        }

        /* Calculate necessary offsets. */
        val address = file.toAddress(tupleId)
        val pageId = address.toPageId()
        val slotId = address.toSlotId()
        val entryOffset: Int = slotId * this.file.entrySize

        val page = this.bufferPool.get(pageId, Priority.DEFAULT)
        try {
            val flags = page.getInt(entryOffset)
            if ((flags and FixedHareColumnFile.MASK_DELETED) > 0) throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be updated.")
            if (value != null) {
                page.putInt(entryOffset, (flags and FixedHareColumnFile.MASK_NULL.inv()))
                this.serializer.serialize(page, entryOffset + FixedHareColumnFile.ENTRY_HEADER_SIZE, value)
            } else {
                page.putInt(entryOffset, (flags or FixedHareColumnFile.MASK_NULL))
                for (i in 0 until this.file.entrySize) {
                    page.putByte(entryOffset + FixedHareColumnFile.ENTRY_HEADER_SIZE + i, 0)
                }
            }
        } finally {
            page.release()
        }
    }


    /**
     * Updates the entry for the given [TupleId] if it is equal to the expected entry.
     *
     * @param tupleId The [TupleId] to update the entry for.
     * @param expectedValue The value [T] the entry is expected to contain.
     * @param newValue The new value [T] the updated entry should contain or null.
     * @return true if update was successful.
     */
    @Synchronized
    override fun compareAndUpdate(tupleId: TupleId, expectedValue: T?, newValue: T?): Boolean = this.localLock.shared {
        /* Check nullability constraint. */
        if (newValue == null && !this.file.nullable) {
            throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
        }

        /* Calculate necessary offsets. */
        val address = file.toAddress(tupleId)
        val pageId = address.toPageId()
        val slotId = address.toSlotId()
        val entryOffset: Int = slotId * this.file.entrySize

        val page = this.bufferPool.get(pageId, Priority.DEFAULT)
        try {
            val flags = page.getInt(entryOffset)
            if ((flags and FixedHareColumnFile.MASK_DELETED) > 0) throw throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be updated.")
            val value = if ((flags and FixedHareColumnFile.MASK_NULL) > 0) {
                null
            } else {
                this.serializer.deserialize(page, entryOffset)
            }
            if (value != expectedValue) {
                return false
            } else {
                if (newValue != null) {
                    page.putInt(entryOffset, (flags and FixedHareColumnFile.MASK_NULL.inv()))
                    this.serializer.serialize(page, entryOffset + FixedHareColumnFile.ENTRY_HEADER_SIZE, newValue)
                } else {
                    page.putInt(entryOffset, flags or FixedHareColumnFile.MASK_NULL)
                    for (i in 0 until this.file.entrySize) {
                        page.putByte(entryOffset + FixedHareColumnFile.ENTRY_HEADER_SIZE + i, 0)
                    }
                }
                return true
            }
        } finally {
            page.release()
        }
    }

    /**
     * Deletes the entry for the given [TupleId].
     *
     * @param tupleId The [TupleId] to delete.
     * @return The value of the entry before deletion.
     *
     * @throws EntryDeletedException If entry identified by [TupleId] has been deleted.
     */
    override fun delete(tupleId: TupleId): T? = this.localLock.shared {
        /* Load page header. */
        val headerPage = this.bufferPool.get(FixedHareColumnFile.ROOT_PAGE_ID, Priority.HIGH)
        val header = HeaderPageView(headerPage)

        /* Calculate necessary offsets. */
        val address = this.file.toAddress(tupleId)
        val pageId = address.toPageId()
        val slotId = address.toSlotId()
        val entryOffset = slotId * this.file.entrySize

        val page = this.bufferPool.get(pageId, Priority.DEFAULT)
        try {
            val flags = page.getInt(entryOffset)
            if ((flags and FixedHareColumnFile.MASK_DELETED) > 0L) throw throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be deleted.")

            /* Retrieve current value. */
            val ret = if ((flags and FixedHareColumnFile.MASK_NULL) > 0L) {
                null
            } else {
                this.serializer.deserialize(page, entryOffset + FixedHareColumnFile.ENTRY_HEADER_SIZE)
            }

            /* Delete entry. */
            page.putInt(entryOffset, (flags or FixedHareColumnFile.MASK_DELETED))
            for (i in 0 until this.file.entrySize) {
                page.putByte(i + FixedHareColumnFile.ENTRY_HEADER_SIZE + entryOffset, 0)
            }

            /* Update header. */
            header.count -= 1
            headerPage.release()

            /* Return deleted value. */
            return ret
        } finally {
            page.release()
        }
    }

    /**
     * Appends the provided [Value] to the underlying [FixedHareColumnFile], assigning it a new [TupleId].
     *
     * @param value The [Value] to append. Can be null, if the [FixedHareColumnFile] permits it.
     * @return The [TupleId] of the new value.
     *
     * @throws NullValueNotAllowedException If [value] is null but the underlying data structure does not support null values.
     */
    override fun append(value: T?): TupleId = this.localLock.shared {
        /* Check nullability constraint. */
        if (value == null && !this.file.nullable) {
            throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
        }

        /* Fetch & wrap header */
        val headerPage = this.bufferPool.get(FixedHareColumnFile.ROOT_PAGE_ID, Priority.HIGH)
        val headerView = HeaderPageView(headerPage).validate()

        /* Calculate necessary offsets. */
        val tupleId = headerView.maxTupleId + 1
        val address = this.file.toAddress(tupleId)
        val pageId = address.toPageId()
        val slotId = address.toSlotId()
        val offset = slotId * this.file.entrySize

        if (pageId > this.bufferPool.totalPages) {
            /* Case 1: Data goes on new pages. */
            this.bufferPool.append()
            val page = this.bufferPool.get(pageId)
            try {
                if (value != null) {
                    page.putLong(offset, 0L)
                    this.serializer.serialize(page, offset + FixedHareColumnFile.ENTRY_HEADER_SIZE, value)
                }
            } finally {
                page.release()
            }
        } else {
            /* Case 2: Data goes on an existing page.*/
            val page = this.bufferPool.get(pageId, Priority.DEFAULT)
            try {
                if (value != null) {
                    page.putLong(offset, 0L)
                    this.serializer.serialize(page, offset + FixedHareColumnFile.ENTRY_HEADER_SIZE, value)
                } else {
                    page.putInt(offset, FixedHareColumnFile.MASK_NULL)
                    for (i in 0 until this.file.entrySize) {
                        page.putByte(offset + FixedHareColumnFile.ENTRY_HEADER_SIZE + i, 0)
                    }
                }
            } finally {
                page.release()
            }
        }

        /* Update header. */
        headerView.maxTupleId = max(tupleId, headerView.maxTupleId)
        headerView.count += 1

        /* Release header page. */
        headerPage.release()

        /* Return TupleId. */
        return tupleId
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
     * Closes this [FixedHareColumnReader].
     */
    override fun close() = this.localLock.exclusive {
        if (this.isOpen) {
            this.isOpen = false
            this.file.releaseLock(this.lockHandle)
        }
    }
}