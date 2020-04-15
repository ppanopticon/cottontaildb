package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.EntryDeletedException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.NullValueNotAllowedException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.TupleId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.TupleIdOutOfBoundException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column.FixedHareColumnFile.Companion.MASK_DELETED
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column.FixedHareColumnFile.Companion.MASK_NULL
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.Cursor
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.PageRef
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.Priority
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.exclusive
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.shared
import java.util.concurrent.locks.StampedLock

/**
 * A [Cursor] for access to the raw entries in a [FixedHareColumnFile].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedHareCursor<T : Value>(columnFile: FixedHareColumnFile<T>, override val writeable: Boolean = false, val bufferSize: Int = 10) : Cursor<T> {

    /** Internal (per-cursor) [BufferPool]. */
    private val bufferPool = BufferPool(columnFile.disk, this.bufferSize)

    /** Size of a single page in bytes. */
    private val pageSize = columnFile.disk.pageSize

    /** Size of a single entry in bytes. */
    private val entrySize = columnFile.entrySize

    /** Number of entries per page. */
    private val fillFactor = Math.floorDiv(this.pageSize, this.entrySize)

    /** Local reference to the [Serializer] used for this [FixedHareCursor]. */
    private val serializer = columnFile.columnDef.serializer

    /** Every [FixedHareCursor] needs access to the [FixedHareColumnFile.Header]. */
    private val header = columnFile.Header()

    /** The maximum [TupleId] supported by this [FixedHareCursor]. */
    override val maximum: TupleId
        get() = this.header.count - 1

    /** Internal lock to mediate access to closing the [FixedHareCursor]. */
    private val closeLock = StampedLock()

    /** Internal flag used to indicate, that this [FixedHareCursor] was closed. */
    @Volatile
    private var closed: Boolean = false

    /**
     * Returns a boolean indicating whether the entry the the current [FixedHareCursor] position is null.
     *
     * @return true if the entry at the current position of the [FixedHareCursor] is null and false otherwise.
     */
    override fun isNull(tupleId: TupleId): Boolean = this.closeLock.shared {
        check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

        val address = this.tupleIdToAddress(tupleId)
        val page = this.bufferPool.get(address.first)

        return (page.getLong(address.second) and MASK_NULL) > 0L
    }

    /**
     * Returns a boolean indicating whether the entry the the current [FixedHareCursor] position has been deleted.
     *
     * @return true if the entry at the current position of the [FixedHareCursor] has been deleted and false otherwise.
     */
    override fun isDeleted(tupleId: TupleId): Boolean = this.closeLock.shared  {
        check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

        val address = this.tupleIdToAddress(tupleId)
        val page = this.bufferPool.get(address.first)

        return (page.getLong(address.second) and MASK_DELETED) > 0L
    }

    /**
     * Returns the entry for the given [TupleId] if such an entry exists.
     *
     * @param tupleId The [TupleId] to return the entry for.
     * @return Entry [T] for the given [TupleId].
     */
    override fun get(tupleId: TupleId): T? = this.closeLock.shared {
        check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

        val address = this.tupleIdToAddress(tupleId)
        val page = this.bufferPool.get(address.first)
        val flags = page.getLong(address.second)

        return try {
            if ((flags and MASK_DELETED) > 0L) throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be accessed.")
            if ((flags and MASK_NULL) > 0L) return null
            this.serializer.deserialize(page, address.second + FixedHareColumnFile.ENTRY_HEADER_SIZE)
        } finally {
            page.release()
        }
    }

    /**
     * Updates the entry for the given [TupleId]
     *
     * @param tupleId The [TupleId] to return the entry for.
     * @param value The new value [T] the updated entry should contain or null.
     */
    override fun update(tupleId: TupleId, value: T?) = this.closeLock.shared {
        check(this.writeable) { "HareCursor is a read-only cursor and cannot be used to write data." }
        check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

        /* Check nullability constraint. */
        if (value == null && !this.header.nullable) {
            throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
        }

        val address = this.tupleIdToAddress(tupleId)
        val page = this.bufferPool.get(address.first)
        try {
            val flags = page.getLong(address.second)
            if ((flags and MASK_DELETED) > 0L) throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be updated.")
            if (value != null) {
                page.putLong(address.second, (flags and MASK_NULL.inv()))
                this.serializer.serialize(page, address.second + FixedHareColumnFile.ENTRY_HEADER_SIZE, value)
            } else {
                page.putLong(address.second, (flags or MASK_NULL))
                for (i in (address.second + FixedHareColumnFile.ENTRY_HEADER_SIZE)..this.entrySize) {
                    page.putByte(i, 0)
                }
            }
        } finally {
            page.release()
        }
    }

    /**
     * Updates the entry for the given [TupleId] if it is equal to the expected entry.
     *
     * @param tupleId The [TupleId] to return the entry for.
     * @param expectedValue The value [T] the entry is expected to contain.
     * @param newValue The new value [T] the updated entry should contain or null.
     */
    override fun compareAndUpdate(tupleId: TupleId, expectedValue: T?, newValue: T?): Boolean = this.closeLock.shared {
        check(this.writeable) { "HareCursor is a read-only cursor and cannot be used to write data." }
        check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

        /* Check nullability constraint. */
        if (newValue == null && !this.header.nullable) {
            throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
        }

        val address = this.tupleIdToAddress(tupleId)
        val page = this.bufferPool.get(address.first)

        try {
            val flags = page.getLong(address.second)
            if ((flags and MASK_DELETED) > 0L) throw throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be updated.")
            val value = if ((flags and MASK_NULL) > 0L) {
                null
            } else {
                this.serializer.deserialize(page, address.second + FixedHareColumnFile.ENTRY_HEADER_SIZE)
            }

            if (value != expectedValue) return false
            if (newValue != null) {
                page.putLong(address.second, (flags and MASK_NULL.inv()))
                this.serializer.serialize(page, address.second + FixedHareColumnFile.ENTRY_HEADER_SIZE, newValue)
            } else {
                page.putLong(address.second, (flags or MASK_NULL))
                for (i in (address.second + FixedHareColumnFile.ENTRY_HEADER_SIZE)..this.entrySize) {
                    page.putByte(i, 0)
                }
            }
            return true
        } finally {
            page.release()
        }
    }

    override fun append(value: T?): TupleId = this.closeLock.shared {
        check(this.writeable) { "HareCursor is a read-only cursor and cannot be used to write data." }
        check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

        /* Check nullability constraint. */
        if (value == null && !this.header.nullable) {
            throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
        }

        val tupleId = this.header.count
        val pageId = (tupleId / this.fillFactor) + 1
        val relativeOffset = this.entrySize * ((tupleId % this.fillFactor).toInt())
        this.header.count++

        if (pageId >= this.bufferPool.totalPages) {
            /* Case 1: Data goes on new pages. */
            val page = this.bufferPool.detach()
            if (value != null) {
                page.putLong(0, 0L)
                this.serializer.serialize(page, FixedHareColumnFile.ENTRY_HEADER_SIZE, value)
            } else {
                page.putLong(0, MASK_NULL)
                for (i in (FixedHareColumnFile.ENTRY_HEADER_SIZE)..this.entrySize) {
                    page.putByte(i, 0)
                }
            }
            this.bufferPool.append(page)
            page.release()
        } else {
            /* Case 2: Data goes on an existing page.*/
            val page = this.bufferPool.get(pageId, Priority.DEFAULT)
            if (value != null) {
                page.putLong(relativeOffset,0L)
                this.serializer.serialize(page, relativeOffset + FixedHareColumnFile.ENTRY_HEADER_SIZE, value)
            } else {
                page.putLong(relativeOffset, MASK_NULL)
                for (i in (relativeOffset + FixedHareColumnFile.ENTRY_HEADER_SIZE)..this.entrySize) {
                    page.putByte(i, 0)
                }
            }
            page.release()
        }

        return tupleId
    }

    override fun delete(tupleId: TupleId): T? = this.closeLock.shared {
        check(this.writeable) { "HareCursor is a read-only cursor and cannot be used to write data." }
        check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

        val address = this.tupleIdToAddress(tupleId)
        val page = this.bufferPool.get(address.first)
        try {
            val flags = page.getLong(address.second)
            if ((flags and MASK_DELETED) > 0L) throw throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be deleted.")

            /* Retrieve current value. */
            val ret = if ((flags and MASK_NULL) > 0L) {
                return null
            } else {
                this.serializer.deserialize(page, address.second + FixedHareColumnFile.ENTRY_HEADER_SIZE)
            }

            /* Delete entry. */
            page.putLong(address.second, (flags or MASK_DELETED))
            for (i in (address.second + FixedHareColumnFile.ENTRY_HEADER_SIZE)..this.entrySize) {
                page.putByte(i, 0)
            }

            /* Return deleted value. */
            return ret
        } finally {
            page.release()
        }
    }


    /**
     * Closes this [FixedHareCursor] and releases all resources associated with hit.
     */
    override fun close() = this.closeLock.exclusive {
        if (!this.closed) {
            this.bufferPool.close()
            this.closed = true
        }
    }

    /**
     * Iterates over the given range of [TupleIds and executed the provided [action] for each entry.
     *
     * @param start The start [TupleId] for the iteration. Defaults to 0
     * @param end The end [TupleId] for the iteration. Defaults to [FixedHareCursor.maximum]
     * @param action The action that should be executed.
     */
    override fun forEach(start: TupleId, end: TupleId, action: (TupleId, T?) -> Unit) = this.internalIterator(start, end, action)

    /**
     * Iterates over the given range of [TupleId]s and executes the provided mapping [action] for each entry.
     *
     * @param start The start [TupleId] for the iteration. Defaults to 0
     * @param end The end [TupleId] for the iteration. Defaults to [FixedHareCursor.maximum]
     * @param action The action that should be executed.
     */
    override fun <R> map(start: TupleId, end: TupleId, action: (TupleId, T?) -> R?) = this.internalIterator(start, end, action)

    /**
     * Internal function that facilitates iteration over entries.
     *
     * @param start
     * @param end
     * @param action
     */
    private inline fun <R> internalIterator(start: TupleId, end: TupleId, action: (TupleId, T?) -> R) = this.closeLock.shared {
        check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

        /* Sanity checks. */
        require(end >= start) { "End-index for iteration must be greater or equal to start index. "}
        if (end < 0L || end > this.maximum) {
            throw TupleIdOutOfBoundException("Tuple ID $end is out of bounds for this HARE column (maximum=${this.maximum})")
        }

        /* Determine maximum and minimum page ID. */
        val prefetch = (this.bufferPool.size - 1)
        val minPageId = ((start / this.fillFactor) + 1)
        val maxPageId = ((end / this.fillFactor) + 1)
        val maxRelativeOffset = (this.fillFactor * this.entrySize)
        var i = start

        /* Start iteration. */
        for (pageId in minPageId..maxPageId) {
            val page: PageRef = this.bufferPool.get(pageId)
            try {
                for (relativeOffset in 0 until maxRelativeOffset step this.entrySize) {
                    val flags = page.getLong(relativeOffset)
                    if ((flags and MASK_DELETED) > 0L) continue
                    if ((flags and MASK_NULL) > 0L) {
                        action(i, null)
                    } else {
                        action(i, this.serializer.deserialize(page, relativeOffset + FixedHareColumnFile.ENTRY_HEADER_SIZE))
                    }

                    /* Abort if end has been reached. */
                    if ((++i) == end) {
                        return@shared
                    }
                }

                /* Tell buffer pool to prefetch pages. */
                if (pageId % prefetch == 0L) {
                    this.bufferPool.prefetch((pageId+1)..(pageId + prefetch))
                }
            } finally {
                page.release()
            }
        }
    }

    /**
     * Converts the given [TupleId] to an address.
     *
     * @param tupleId The [TupleId] to convert.
     * @return Address for the tuple identified by the [TupleId].
     */
    private fun tupleIdToAddress(tupleId: TupleId): Pair<PageId,Int> {
        if (tupleId < 0L || tupleId > this.maximum) {
            throw TupleIdOutOfBoundException("Tuple ID $tupleId is out of bounds for this HARE column (maximum=${this.maximum})")
        }
        return Pair(((tupleId / this.fillFactor) + 1), this.entrySize * ((tupleId % this.fillFactor).toInt()))
    }
}