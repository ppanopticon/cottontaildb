package org.vitrivr.cottontail.storage.engine.hare.access.column

import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.*
import org.vitrivr.cottontail.storage.engine.hare.access.EntryDeletedException
import org.vitrivr.cottontail.storage.engine.hare.access.NullValueNotAllowedException
import org.vitrivr.cottontail.storage.engine.hare.access.TupleIdOutOfBoundException
import org.vitrivr.cottontail.storage.engine.hare.access.cursor.Cursor
import org.vitrivr.cottontail.storage.engine.hare.basics.PageRef
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionPolicy
import org.vitrivr.cottontail.storage.engine.hare.disk.DataPage
import org.vitrivr.cottontail.storage.engine.hare.disk.DirectDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.wal.WALDiskManager
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.shared
import org.vitrivr.cottontail.utilities.math.BitUtil

import java.lang.StrictMath.floorDiv
import java.lang.StrictMath.max
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.locks.StampedLock

/**
 * A HARE column file where each entry has a fixed size and can be addressed by a [TupleId].
 *
 * @author Ralph Gasser
 * @param 1.0
 */
class FixedHareColumnFile <T: Value>(val path: Path, wal: Boolean, corePoolSize: Int = 5) : AutoCloseable {

    /** Companion object with important constants. */
    companion object {

        /** Identifier for HARE_COLUMN_HEADER. *H*are *C*olumn *F*ixed */
        val FILE_HEADER_IDENTIFIER = charArrayOf('H', 'C', 'F')

        /** [PageId] of the root [DataPage]. */
        const val ROOT_PAGE_ID: PageId = 0

        /** Mask for 'NULLABLE' bit in [FixedHareColumnFile.Header]. */
        const val MASK_NULLABLE = 1L shl 0

        /** Size of an entry's header in bytes. */
        const val ENTRY_HEADER_SIZE = 4

        /** Mask for 'NULL' bit in each [FixedHareColumnFile] entry. */
        const val MASK_NULL = 1 shl 1

        /** Mask for 'DELETED' bit in each [FixedHareColumnFile] entry. */
        const val MASK_DELETED = 1 shl 2

        /**
         * Creates a new [FixedHareColumnFile] under the given location.
         *
         * @param path [Path] to the directory in which to create the [FixedHareColumnFile].
         * @param columnDef The [ColumnDef] that describes this [FixedHareColumnFile].
         * @param desiredFillFactor The desired number of entries per page (fill factor = (entrySize / pageSize)). Used to determine the size of a page.
         */
        fun createDirect(path: Path, columnDef: ColumnDef<*>, desiredFillFactor: Int = 8) {
            val entrySize = columnDef.serializer.physicalSize + ENTRY_HEADER_SIZE
            val pageShift = determinePageSize(entrySize)
            DiskManager.create(path, pageShift)

            val manager = DirectDiskManager(path, 5000)
            val headerPage = DataPage(ByteBuffer.allocate(manager.pageSize))
            headerPage.putChar(0, FILE_HEADER_IDENTIFIER[0])                                        /* 0: Identifier H. */
            headerPage.putChar(2, FILE_HEADER_IDENTIFIER[1])                                        /* 2: Identifier C. */
            headerPage.putChar(4, FILE_HEADER_IDENTIFIER[2])                                        /* 4: Identifier F. */

            headerPage.putInt(6, columnDef.type.ordinal)                                            /* 6: Type of column. See ColumnDef.forOrdinal() */
            headerPage.putInt(10, columnDef.logicalSize)                                                   /* 10: Logical size of column (for structured data types). */
            headerPage.putInt(14, columnDef.serializer.physicalSize)                                /* 14: Physical size of a column entry in bytes. */
            headerPage.putLong(18, if (columnDef.nullable) { (0L or MASK_NULLABLE) } else { 0L })   /* 18: Column flags; 64 bits, one bit reserved. */
            headerPage.putLong(26, 0L)                                                        /* 26: Number of entries (count) in column. */

            manager.allocate(headerPage)
            manager.close()
        }

        /**
         * Determines the optimal size of page based on the size of an entry and the desired fill factor.
         *
         * @param entrySize Size of an entry in bytes.
         * @param desiredFillFactor Desired fill factor (i.e., entries per page)
         */
        private fun determinePageSize(entrySize: Int): Int {
            var pageShift = max(BitUtil.toShift(BitUtil.nextPowerOfTwo(entrySize)), DiskManager.MIN_PAGE_SHIFT)
            var waste = (1 shl pageShift) - entrySize * floorDiv((1 shl pageShift), entrySize)
            for (i in (pageShift+1)..DiskManager.MAX_PAGE_SHIFT) {
                val newWaste = (1 shl i) - entrySize * floorDiv((1 shl i), entrySize)
                if (newWaste < waste) {
                    waste = newWaste
                    pageShift = i
                }
            }
            return pageShift
        }
    }

    /** Initializes the [DiskManager] based on the `wal` property. */
    val disk = if (wal) { WALDiskManager(this.path) } else { DirectDiskManager(this.path) }

    /** Initializes the [BufferPool]. */
    val bufferPool = BufferPool(disk = this.disk, size = corePoolSize, evictionPolicy = EvictionPolicy.LRU)

    /** The [Name] of this [FixedHareColumnFile]. */
    val name = Name.ColumnName(this.path.fileName.toString().replace(".db", ""))

    /** The [ColumnDef] describing the column managed by this [FixedHareColumnFile]. */
    val columnDef: ColumnDef<T>

    /** Internal lock to mediate access to closing the [FixedHareCursor]. */
    private val closeLock = StampedLock()

    /** Internal flag used to indicate, that this [FixedHareCursor] was closed. */
    @Volatile
    private var closed: Boolean = false

    /* Initialize important fields. */
    init {
        val header = Header()
        this.columnDef = ColumnDef(this.name, header.type, header.size, (header.flags and MASK_NULLABLE) != 0L)
    }

    /**
     * Creates and returns a [Cursor] for this [FixedHareColumnFile]. The [Cursor] can be used to manipulate the entries in this [FixedHareColumnFile]
     *
     * @param writeable True if the [Cursor] should be writeable.
     * @param bufferSize The size of the [BufferPool] that backs the new [Cursor].
     *
     * @return The [FixedHareCursor] that can be used to alter the given [TupleId]
     */
    fun cursor(writeable: Boolean = false, bufferSize: Int = 10): Cursor<T> = FixedHareCursor(writeable, bufferSize)

    /**
     * Closes this [FixedHareColumnFile].
     */
    override fun close() = this.closeLock.exclusive {
        if (!this.disk.isOpen) {
            this.bufferPool.close()
            this.disk.close()
            this.closed = true
        }
    }

    /**
     * The [Header] of this [FixedHareColumnFile]. The [Header] is located on the first [DataPage] in the [FixedHareColumnFile] file.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class Header {
        /** The [ColumnType] held by this [FixedHareColumnFile]. */
        val type: ColumnType<T>

        /** The logical size of the [ColumnDef] held by this [FixedHareColumnFile]. */
        val size: Int

        /** The size of an entry in bytes. */
        val entrySize: Int

        /** The number of slots per data page. */
        val slots: Int

        /** Special flags set for this [FixedHareColumnFile], such as, nullability. */
        val flags: Long

        /** True if this [FixedHareColumnFile] supports null values. */
        val nullable: Boolean
            get() = ((this.flags and MASK_NULLABLE) > 0L)

        /** The total number of entries in this [FixedHareColumnFile]. */
        var count: Long
            get() {
                val page = this@FixedHareColumnFile.bufferPool.get(ROOT_PAGE_ID, Priority.HIGH)
                return page.getLong(26)
            }
            set(v) {
                val page = this@FixedHareColumnFile.bufferPool.get(ROOT_PAGE_ID, Priority.HIGH)
                page.putLong(26, v)
            }

        /** The number of deleted entries in this [FixedHareColumnFile]. */
        var deleted: Long
            get() {
                val page = this@FixedHareColumnFile.bufferPool.get(ROOT_PAGE_ID, Priority.HIGH)
                return page.getLong(34)
            }
            set(v) {
                val page = this@FixedHareColumnFile.bufferPool.get(ROOT_PAGE_ID, Priority.HIGH)
                page.putLong(34, v)
            }

        /** Make necessary check on initialization. */
        init {
            val page = this@FixedHareColumnFile.bufferPool.get(ROOT_PAGE_ID, Priority.HIGH)
            require(page.getChar(0) == FILE_HEADER_IDENTIFIER[0]) { DataCorruptionException("Identifier mismatch in HARE fixed column file (file: ${this@FixedHareColumnFile.path.fileName}).") }
            require(page.getChar(2) == FILE_HEADER_IDENTIFIER[1]) { DataCorruptionException("Identifier mismatch in HARE fixed column file (file: ${this@FixedHareColumnFile.path.fileName}).") }
            require(page.getChar(4) == FILE_HEADER_IDENTIFIER[2]) { DataCorruptionException("Identifier mismatch in HARE fixed column file (file: ${this@FixedHareColumnFile.path.fileName}).") }
            try {
                this.type = ColumnType.forOrdinal(page.getInt(6)) as ColumnType<T>
            } catch (e: IllegalArgumentException) {
                throw DataCorruptionException("Column type ordinal mismatch in HARE fixed column file ${this@FixedHareColumnFile.path.fileName}.")
            }
            this.size = page.getInt(10)
            this.entrySize = page.getInt(14)
            require(this.entrySize  <= (1 shl this@FixedHareColumnFile.disk.pageShift)) { DataCorruptionException("Entry size mismatch in HARE fixed column file; entry size must be smaller or equal to page size ${this@FixedHareColumnFile.path.fileName}.") }
            this.slots = floorDiv(1 shl this@FixedHareColumnFile.disk.pageShift, (this.entrySize + ENTRY_HEADER_SIZE))
            this.flags = page.getLong(18)
            require(page.getInt(26) >= 0) { DataCorruptionException("Negative number of entries in HARE fixed column file ${this@FixedHareColumnFile.path.fileName}.") }
        }
    }

    /**
     * A [Cursor] for access to the raw entries in a [FixedHareColumnFile].
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class FixedHareCursor(override val writeable: Boolean, val bufferSize: Int) : Cursor<T> {
        /** Internal (per-cursor) [BufferPool]. */
        private val bufferPool = BufferPool(this@FixedHareColumnFile.disk, this.bufferSize, EvictionPolicy.FIFO)

        /** Local reference to the [Serializer] used for this [FixedHareCursor]. */
        private val serializer = this@FixedHareColumnFile.columnDef.serializer

        /** Every [FixedHareCursor] needs access to the [FixedHareColumnFile.Header]. */
        private val header = this@FixedHareColumnFile.Header()

        /** Acquires a latch on the outer [FixedHareColumnFile]. This latch remains active until [FixedHareCursor] is released. */
        private val outerCloseStamp = this@FixedHareColumnFile.closeLock.readLock()

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
            val page = this.bufferPool.get(address.page())

            try {
                return (page.getInt(slotIdToEntryOffset(address.slot())) and MASK_NULL) > 0L
            } finally {
                page.release()
            }
        }

        /**
         * Returns a boolean indicating whether the entry the the current [FixedHareCursor] position has been deleted.
         *
         * @return true if the entry at the current position of the [FixedHareCursor] has been deleted and false otherwise.
         */
        override fun isDeleted(tupleId: TupleId): Boolean = this.closeLock.shared  {
            check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

            val address = this.tupleIdToAddress(tupleId)
            val page = this.bufferPool.get(address.page())

            try {
                return (page.getInt(slotIdToEntryOffset(address.slot())) and MASK_DELETED) > 0L
            } finally {
                page.release()
            }
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
            val page = this.bufferPool.get(address.page())

            try {
                val flags = page.getInt(slotIdToHeaderOffset(address.slot()))
                if ((flags and MASK_DELETED) > 0L) throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be accessed.")
                if ((flags and MASK_NULL) > 0L) {
                    null
                } else {
                    this.serializer.deserialize(page, slotIdToEntryOffset(address.slot()))
                }
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
            val page = this.bufferPool.get(address.page())
            try {
                val headerOffset = slotIdToHeaderOffset(address.slot())
                val entryOffset = slotIdToEntryOffset(address.slot())
                val flags = page.getInt(headerOffset)
                if ((flags and MASK_DELETED) > 0) throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be updated.")
                if (value != null) {
                    page.putInt(headerOffset, (flags and MASK_NULL.inv()))
                    this.serializer.serialize(page, entryOffset, value)
                } else {
                    page.putInt(headerOffset, (flags or MASK_NULL))
                    for (i in 0 until this.header.entrySize) {
                        page.putByte(entryOffset + i, 0)
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
            val page = this.bufferPool.get(address.page())
            try {
                val headerOffset = slotIdToHeaderOffset(address.slot())
                val entryOffset = slotIdToEntryOffset(address.slot())
                val flags = page.getInt(headerOffset)
                if ((flags and MASK_DELETED) > 0) throw throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be updated.")
                val value = if ((flags and MASK_NULL) > 0) {
                    null
                } else {
                    this.serializer.deserialize(page, entryOffset)
                }
                if (value != expectedValue) {
                    false
                } else {
                    if (newValue != null) {
                        page.putInt(headerOffset, (flags and MASK_NULL.inv()))
                        this.serializer.serialize(page, entryOffset, newValue)
                    } else {
                        page.putInt(headerOffset, flags or MASK_NULL)
                        for (i in 0 until this.header.entrySize) {
                            page.putByte(entryOffset+i, 0)
                        }
                    }
                    true
                }
            } finally {
                page.release()
            }
        }

        /**
         *
         */
        override fun append(value: T?): TupleId = this.closeLock.shared {
            check(this.writeable) { "HareCursor is a read-only cursor and cannot be used to write data." }
            check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

            /* Check nullability constraint. */
            if (value == null && !this.header.nullable) {
                throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
            }

            val tupleId = this.header.count++
            val address = this.tupleIdToAddress(tupleId)

            if (address.page() >= this.bufferPool.totalPages) {
                /* Case 1: Data goes on new pages. */
                val page = this.bufferPool.detach()
                try {
                    if (value != null) {
                        page.putLong(slotIdToHeaderOffset(address.slot()), 0L)
                        this.serializer.serialize(page, slotIdToEntryOffset(address.slot()), value)
                    }
                    this.bufferPool.append(page)
                } finally {
                    page.release()
                }
            } else {
                /* Case 2: Data goes on an existing page.*/
                val page = this.bufferPool.get(address.page(), Priority.DEFAULT)
                try {
                    if (value != null) {
                        page.putLong(slotIdToHeaderOffset(address.slot()), 0L)
                        this.serializer.serialize(page, slotIdToEntryOffset(address.slot()), value)
                    } else {
                        val entryOffset = slotIdToEntryOffset(address.slot())
                        page.putInt(slotIdToHeaderOffset(address.slot()), MASK_NULL)
                        for (i in 0 until this.header.entrySize) {
                            page.putByte(entryOffset + i, 0)
                        }
                    }
                } finally {
                    page.release()
                }
            }
            return tupleId
        }

        override fun delete(tupleId: TupleId): T? = this.closeLock.shared {
            check(this.writeable) { "HareCursor is a read-only cursor and cannot be used to write data." }
            check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

            val address = this.tupleIdToAddress(tupleId)
            val page = this.bufferPool.get(address.page())

            try {
                val headerOffset = slotIdToHeaderOffset(address.slot())
                val entryOffset = slotIdToEntryOffset(address.slot())
                val flags = page.getInt(headerOffset)
                if ((flags and MASK_DELETED) > 0L) throw throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be deleted.")

                /* Retrieve current value. */
                val ret = if ((flags and MASK_NULL) > 0L) {
                    null
                } else {
                    this.serializer.deserialize(page, entryOffset)
                }

                /* Delete entry. */
                page.putInt(headerOffset, (flags or MASK_DELETED))
                for (i in 0 until this.header.entrySize) {
                    page.putByte(i + entryOffset, 0)
                }

                /* Return deleted value. */
                ret
            } finally {
                page.release()
            }
        }

        /** Converts a [SlotId] to an offset into the [Page]. */
        private fun slotIdToHeaderOffset(slotId: Int) = (slotId shl 2)

        /** Converts a [SlotId] to an offset into the [Page]. */
        private fun slotIdToEntryOffset(slotId: Int) = this@FixedHareColumnFile.disk.pageSize - ((slotId + 1) * this.header.entrySize)

        /**
         * Closes this [FixedHareCursor] and releases all resources associated with hit.
         */
        override fun close() = this.closeLock.exclusive {
            if (!this.closed) {
                this.bufferPool.close()
                this@FixedHareColumnFile.closeLock.unlock(this.outerCloseStamp)
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
        override fun <R> map(start: TupleId, end: TupleId, action: (TupleId, T?) -> R?): Collection<R?> {
            val out = mutableListOf<R?>()
            internalIterator(start, end) { t, v ->
                out.add(action(t,v))
            }
            return out
        }

        /**
         * Converts the given [TupleId] to an address that points to the correct location within this [FixedHareColumnFile].
         *
         * @param tupleId The [TupleId] to convert.
         * @return [Address] for the tuple identified by the [TupleId] and the [SlotId].
         */
        private fun tupleIdToAddress(tupleId: TupleId): Address = longArrayOf((tupleId / this.header.slots) + 1, (tupleId % this.header.slots))

        /**
         * Internal function that facilitates iteration over entries.
         *
         * @param start
         * @param end
         * @param action
         */
        private inline fun <R> internalIterator(start: TupleId, end: TupleId, crossinline action: (TupleId, T?) -> R) = this.closeLock.shared {
            check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

            /* Sanity checks. */
            require(end >= start) { "End-index for iteration must be greater or equal to start index (start: $start, end: $end)."}
            if (end < 0L || end > this.maximum) {
                throw TupleIdOutOfBoundException("Tuple ID $end is out of bounds for this HARE column (maximum=${this.maximum})")
            }

            val prefetch = this.bufferPool.size / 2
            var prefetchCounter = 0
            val entrySize = this.header.entrySize
            val minPageId = (start / this.header.slots) + 1
            val maxPageId = (end / this.header.slots) + 1
            var tupleId = start

            /* Start iteration. */
            for (pageId in minPageId until maxPageId) {
                /* Tell buffer pool to prefetch pages. */
                if (prefetchCounter == 0) {
                    this.bufferPool.prefetch(pageId + prefetch until (pageId + 2 * prefetch))
                    prefetchCounter = prefetch
                }

                /* Access current page id. */
                val page: PageRef = this.bufferPool.get(pageId)
                try {
                    val slot = (entrySize * (tupleId % this.header.slots)).toInt()
                    for (i in slot until this.header.slots) {
                        val flags = page.getInt(slotIdToHeaderOffset(i))
                        if ((flags and MASK_DELETED) == 0) {
                            if ((flags and MASK_NULL) > 0) {
                                action(tupleId, null)
                            } else {
                                action(tupleId, this.serializer.deserialize(page, slotIdToEntryOffset(i)))
                            }
                        }
                        (tupleId++)
                    }
                    prefetchCounter -= 1
                } finally {
                    page.release()
                }
            }
        }
    }
}