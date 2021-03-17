package org.vitrivr.cottontail.storage.engine.hare.access.column.fixed

import org.vitrivr.cottontail.model.basics.TransactionId
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.EntryDeletedException
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnReader
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer
import org.vitrivr.cottontail.storage.engine.hare.toPageId
import org.vitrivr.cottontail.storage.engine.hare.toSlotId
import org.vitrivr.cottontail.storage.serializers.hare.HareSerializer
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.shared
import java.util.concurrent.locks.StampedLock

/**
 * A [HareColumnReader] implementation for [FixedHareColumnFile]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FixedHareColumnReader<T : Value>(val file: FixedHareColumnFile<T>, private val bufferPool: BufferPool) : HareColumnReader<T> {

    /** The [TransactionId] this [FixedHareColumnReader] is associated with. */
    override val tid: TransactionId
        get() = this.bufferPool.tid

    /** Flag indicating whether this [FixedHareColumnReader] is open.  */
    @Volatile
    override var isOpen: Boolean = true
        private set

    /** The [Serializer] used to read data through this [FixedHareColumnReader]. */
    private val serializer: HareSerializer<T> = this.file.type.serializerFactory().hare(this.file.type.logicalSize)

    /** A [StampedLock] that mediates access to methods of this [FixedHareColumnReader]. */
    private val closeLock = StampedLock()

    /** Obtains a lock on the [FixedHareColumnFile]. */
    private val lockHandle = this.file.obtainLock()

    init {
        require(this.file.isOpen) { "FixedHareColumnFile has been closed (file = ${this.file.path})." }
        require(this.file.disk == this.bufferPool.disk) { "FixedHareColumnFile and provided BufferPool do not share the same HareDiskManager." }
    }

    /**
     * Returns the entry for the given [TupleId] if such an entry exists.
     *
     * @param tupleId The [TupleId] to retrieve the entry for.
     * @return Entry [T] for the given [TupleId].
     */
    override fun get(tupleId: TupleId): T? = this.closeLock.shared {
        /* Calculate necessary offsets. */
        val address = file.toAddress(tupleId)
        val pageId = address.toPageId()
        val slotId = address.toSlotId()
        val offset = this.file.pageHeaderSize + slotId * this.file.entrySize

        this.bufferPool.get(pageId).withReadLock { p ->
            val page = SlottedPageView(p)
            if (page.isDeleted(slotId)) throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be accessed.")
            if (page.isNull(slotId)) {
                null
            } else {
                this.serializer.deserialize(page.page, offset)
            }
        }
    }

    /**
     * Returns the number of entries for the [HareColumnFile] backing this [HareColumnReader].
     *
     * @return Number of entries in this [HareColumnFile].
     */
    override fun count(): Long = this.closeLock.shared {
        this.bufferPool.get(FixedHareColumnFile.ROOT_PAGE_ID).withReadLock { h ->
            HeaderPageView(h).count
        }
    }

    /**
     * Returns the maximum [TupleId] for the [HareColumnFile] backing this [HareColumnReader].
     *
     * @return The maximum [TupleId].
     */
    override fun maxTupleId(): TupleId = this.closeLock.shared {
        this.bufferPool.get(FixedHareColumnFile.ROOT_PAGE_ID).withReadLock { h ->
            HeaderPageView(h).maxTupleId
        }
    }

    /**
     * Returns a boolean indicating whether the entry for the given [TupleId] is null.
     *
     * @param tupleId The [TupleId] to check.
     * @return true if the entry for the given [TupleId] is null and false otherwise.
     */
    override fun isNull(tupleId: TupleId): Boolean = this.closeLock.shared {
        /* Calculate necessary offsets. */
        val address = this.file.toAddress(tupleId)
        val pageId = address.toPageId()
        val slotId = address.toSlotId()
        return this.bufferPool.get(pageId).withReadLock { p ->
            SlottedPageView(p).isNull(slotId)
        }
    }

    /**
     * Returns a boolean indicating whether the entry  for the given [TupleId] has been deleted.
     *
     * @return true if the entry for the given [TupleId] has been deleted and false otherwise.
     */
    override fun isDeleted(tupleId: TupleId): Boolean = this.closeLock.shared {
        /* Calculate necessary offsets. */
        val address = this.file.toAddress(tupleId)
        val pageId = address.toPageId()
        val slotId = address.toSlotId()
        return this.bufferPool.get(pageId).withReadLock { p ->
            SlottedPageView(p).isDeleted(slotId)
        }
    }

    /**
     * Closes this [FixedHareColumnReader].
     */
    @Synchronized
    override fun close() = this.closeLock.exclusive {
        if (this.isOpen) {
            this.isOpen = false
            this.file.releaseLock(this.lockHandle)
        }
    }
}

