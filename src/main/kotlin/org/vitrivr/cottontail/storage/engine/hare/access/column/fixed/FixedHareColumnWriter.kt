package org.vitrivr.cottontail.storage.engine.hare.access.column.fixed

import org.vitrivr.cottontail.model.basics.TransactionId
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.SlotId
import org.vitrivr.cottontail.storage.engine.hare.access.EntryDeletedException
import org.vitrivr.cottontail.storage.engine.hare.access.NullValueNotAllowedException
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer
import org.vitrivr.cottontail.storage.engine.hare.toPageId
import org.vitrivr.cottontail.storage.engine.hare.toSlotId
import org.vitrivr.cottontail.storage.serializers.hare.HareSerializer
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.read

import java.util.concurrent.locks.StampedLock

/**
 * A [HareColumnWriter] implementation for [FixedHareColumnFile]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
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
    private val serializer: HareSerializer<T> = this.file.type.serializerFactory().hare(this.file.type.logicalSize)

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
    override fun update(tupleId: TupleId, value: T?) = this.localLock.read {
        /* Check nullability constraint. */
        if (value == null && !this.file.nullable) {
            throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
        }

        /* Calculate necessary offsets. */
        val address = file.toAddress(tupleId)
        val pageId = address.toPageId()
        val slotId = address.toSlotId()

        /* Write value. */
        this.bufferPool.get(pageId).withWriteLock { p ->
            val page = SlottedPageView(p)
            if (page.isDeleted(slotId)) throw throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be updated.")
            this.writeValue(page, slotId, value)

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
    override fun compareAndUpdate(tupleId: TupleId, expectedValue: T?, newValue: T?): Boolean = this.localLock.read {
        /* Check nullability constraint. */
        if (newValue == null && !this.file.nullable) {
            throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
        }

        /* Calculate necessary offsets. */
        val address = file.toAddress(tupleId)
        val pageId = address.toPageId()
        val slotId = address.toSlotId()
        val offset = this.file.pageHeaderSize + slotId * this.file.entrySize

        /* Retrieve current value. */
        this.bufferPool.get(pageId).withWriteLock { p ->
            val page = SlottedPageView(p)
            if (page.isDeleted(slotId)) throw throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be updated.")
            var current: T? = null
            if (!page.isNull(slotId)) {
                current = this.serializer.deserialize(page.page, offset)
            }

            /* Compare current value to expected value; if they match, write value. */
            if (current != expectedValue) return false
            this.writeValue(page, slotId, newValue)
            true
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
    override fun delete(tupleId: TupleId): T? = this.localLock.read {
        /* Calculate necessary offsets. */
        val address = this.file.toAddress(tupleId)
        val pageId = address.toPageId()
        val slotId = address.toSlotId()
        val offset = this.file.pageHeaderSize + slotId * this.file.entrySize

        this.bufferPool.get(FixedHareColumnFile.ROOT_PAGE_ID).withWriteLock {
            /* Update page header. */
            val header = HeaderPageView(it)
            header.updateCount(header.count - 1)

            /* Retrieve current value. */
            this.bufferPool.get(pageId).withWriteLock { p ->
                val page = SlottedPageView(p)
                if (page.isDeleted(slotId)) throw throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be deleted.")
                var old: T? = null
                if (!page.isNull(slotId)) {
                    old = this.serializer.deserialize(page.page, offset)
                }

                /* Override entry with zeros. */
                page.unsetNull(slotId)
                page.setDeleted(slotId)
                for (i in offset until offset + this.file.entrySize) {
                    page.page.putByte(i, 0)
                }
                old
            }
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
    override fun append(value: T?): TupleId = this.localLock.read {
        /* Check nullability constraint. */
        if (!this.file.nullable && value == null) {
            throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
        }
        this.bufferPool.get(FixedHareColumnFile.ROOT_PAGE_ID).withWriteLock { h ->
            val header = HeaderPageView(h)

            /* Calculate necessary offsets. */
            val tupleId = header.maxTupleId + 1
            val address = this.file.toAddress(tupleId)
            val pageId = address.toPageId()
            val slotId = address.toSlotId()

            /* If page does not exists yet, append one. */
            if (pageId > this.file.disk.pages) {
                this.bufferPool.append()
            }

            /* Fetch page and write value. */
            this.bufferPool.get(pageId).withReadLock { s ->
                val page = SlottedPageView(s)
                page.setNoFlag(slotId)
                this.writeValue(page, slotId, value)

                /* Update header. */
                header.updateMaxTupleId(tupleId)
                header.updateCount(header.count + 1)

                /* Return TupleId. */
                tupleId
            }
        }
    }

    /**
     * Commits all changes made through this [HareColumnWriter].
     */
    override fun commit() = this.localLock.read {
        this.bufferPool.flush()
        this.file.disk.commit(this.tid)
    }

    /**
     * Performs a rollback on all changes made through this [HareColumnWriter].
     */
    override fun rollback() = this.localLock.read {
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

    /**
     * Writes the given [value] [T] to the slot with the [SlotId] in the [SlottedPageView].
     *
     * @param page [SlottedPageView] to write to
     * @param slotId The [SlotId] to write to.
     * @param value The value [T] to write. Can be null.
     */
    private fun writeValue(page: SlottedPageView, slotId: SlotId, value: T?) {
        val offset = this.file.pageHeaderSize + slotId * this.file.entrySize
        if (value != null) {
            page.unsetNull(slotId)
            this.serializer.serialize(page.page, offset, value)
        } else {
            page.setNull(slotId)
            for (i in offset until offset + this.file.entrySize) {
                page.page.putByte(i, 0)
            }
        }
    }
}