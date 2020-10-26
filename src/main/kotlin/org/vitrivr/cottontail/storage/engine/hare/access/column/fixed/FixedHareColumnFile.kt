package org.vitrivr.cottontail.storage.engine.hare.access.column.fixed

import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.*
import org.vitrivr.cottontail.storage.engine.hare.access.EntryDeletedException
import org.vitrivr.cottontail.storage.engine.hare.access.NullValueNotAllowedException
import org.vitrivr.cottontail.storage.engine.hare.access.cursor.Cursor
import org.vitrivr.cottontail.storage.engine.hare.access.cursor.ReadableCursor
import org.vitrivr.cottontail.storage.engine.hare.access.cursor.WritableCursor
import org.vitrivr.cottontail.storage.engine.hare.basics.PageRef
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionPolicy
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.DataPage
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
 * A HARE column file where each entry has a fixed size and can be directly addressed by a [TupleId].
 *
 * @author Ralph Gasser
 * @param 1.0.1
 */
class FixedHareColumnFile <T: Value>(val path: Path, wal: Boolean, corePoolSize: Int = 5) : AutoCloseable {

    /** Companion object with important constants. */
    companion object {
        /** [PageId] of the root [DataPage]. */
        const val ROOT_PAGE_ID: PageId = 1L

        /** Constant for beginning of file. */
        const val CURSOR_BOF = -1L

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
        fun createDirect(path: Path, columnDef: ColumnDef<*>) {
            val entrySize = columnDef.serializer.physicalSize + ENTRY_HEADER_SIZE
            val pageShift = determinePageSize(entrySize)
            DiskManager.create(path, pageShift)

            val manager = DirectDiskManager(path, 5000)

            /** Allocate file header page. */
            val page = DataPage(ByteBuffer.allocate(manager.pageSize))
            HeaderPageView().initializeAndWrap(page, columnDef)

            /* Allocate header page. */
            manager.update(manager.allocate(), page)

            /** Allocate first data page. */
            manager.update(manager.allocate(), page.clear())
            manager.close()
        }

        /**
         * Determines the optimal size of page based on the size of an entry and the desired fill factor.
         *
         * @param entrySize Size of an entry in bytes.
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
    val disk = if (wal) {
        WALDiskManager(this.path)
    } else {
        DirectDiskManager(this.path)
    }

    /** Initializes the [BufferPool]. */
    val bufferPool = BufferPool(disk = this.disk, size = corePoolSize, evictionPolicy = EvictionPolicy.LRU)

    /** The [Name] of this [FixedHareColumnFile]. */
    val name = Name.ColumnName(this.path.fileName.toString().replace(".db", ""))

    /** The [ColumnDef] describing the column managed by this [FixedHareColumnFile]. */
    val columnDef: ColumnDef<T>

    /** The number of slots per [Page]. */
    private val slotsPerPage: Int

    /** Internal lock to mediate access to closing the [FixedHareCursor]. */
    private val closeLock = StampedLock()

    /** Internal flag used to indicate, that this [FixedHareCursor] was closed. */
    @Volatile
    private var closed: Boolean = false

    /* Initialize important fields. */
    init {
        val page = this.bufferPool.get(ROOT_PAGE_ID, Priority.HIGH)
        val header = HeaderPageView().wrap(page)
        this.columnDef = ColumnDef(this.name, header.type, header.size, header.nullable) as ColumnDef<T>
        this.slotsPerPage = floorDiv(1 shl this@FixedHareColumnFile.disk.pageShift, (header.entrySize + ENTRY_HEADER_SIZE))
        page.release()
    }

    /**
     * Creates and returns a [ReadableCursor] for this [FixedHareColumnFile]. The [ReadableCursor] can be
     * used to access the entries in this [FixedHareColumnFile]
     *
     * @param bufferSize The size of the [BufferPool] that backs the new [Cursor].
     *
     * @return The [WritableCursor] that can be used to alter the given [TupleId]
     */
    fun cursor(bufferSize: Int = 10): ReadableCursor<T> = FixedHareCursor(false, bufferSize)

    /**
     * Creates and returns a [WritableCursor] for this [FixedHareColumnFile]. The [WritableCursor] can be
     * used to access and manipulate the entries in this [FixedHareColumnFile]
     *
     * @param bufferSize The size of the [BufferPool] that backs the new [Cursor].
     *
     * @return The [WritableCursor] that can be used to alter the given [TupleId]
     */
    fun writableCursor(bufferSize: Int = 10): WritableCursor<T> = FixedHareCursor(true, bufferSize)

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
     * A [Cursor] for access to the raw entries in a [FixedHareColumnFile].
     *
     * @author Ralph Gasser
     * @version 1.0.1
     */
    inner class FixedHareCursor(val writeable: Boolean, val bufferSize: Int) : ReadableCursor<T>, WritableCursor<T> {
        /** Internal (per-cursor) [BufferPool]. */
        private val bufferPool = BufferPool(this@FixedHareColumnFile.disk, this.bufferSize, EvictionPolicy.FIFO)

        /** Local reference to the [Serializer] used for this [FixedHareCursor]. */
        private val serializer = this@FixedHareColumnFile.columnDef.serializer

        /** Every [FixedHareCursor] needs access to the [HeaderPageView]. */
        private val header = HeaderPageView().wrap(this.bufferPool.get(ROOT_PAGE_ID, Priority.HIGH))

        /** Acquires a latch on the outer [FixedHareColumnFile]. This latch remains active until [FixedHareCursor] is released. */
        private val outerCloseStamp = this@FixedHareColumnFile.closeLock.readLock()

        /** The maximum [TupleId] supported by this [FixedHareCursor]. */
        override val maximum: TupleId
            get() = this.header.count

        /** Internal lock to mediate access to closing the [FixedHareCursor]. */
        private val closeLock = StampedLock()

        /** Internal flag used to indicate, that this [FixedHareCursor] was closed. */
        @Volatile
        private var closed: Boolean = false

        /** The [TupleId] this [FixedHareCursor] is currently pointing to. */
        override var tupleId: TupleId = CURSOR_BOF
            private set

        /** The [Address] this [FixedHareCursor] is currently pointing to. */
        val address: Address
            get() = this.tupleId.toAddress()

        /** The [PageId] this [FixedHareCursor] is currently pointing to. */
        val pageId: PageId
            get() = this.address.toPageId()

        /** The [SlotId] this [FixedHareCursor] is currently pointing to. */
        val slotId: SlotId
            get() = this.address.toSlotId()

        /** The offset into the [Page] to access the entry this [FixedHareCursor] is currently pointing to. */
        private val entryOffset: Int
            get() = this.slotId * this.header.entrySize

        /**
         * Moves this [FixedHareCursor] to the next [TupleId].
         *
         * @return True, if [FixedHareCursor] has been moved, false otherwise.
         */
        override fun next(): Boolean = this.seek(this.tupleId + 1)

        /**
         * Moves this [FixedHareCursor] to the previous [TupleId].
         *
         * @return True, if [FixedHareCursor] has been moved, false otherwise.
         */
        override fun previous(): Boolean = this.seek(this.tupleId - 1)

        /**
         * Seeks the given [TupleId] and moves this [FixedHareCursor]'s position to it.
         *
         * @return True, if [FixedHareCursor] has been moved, false otherwise.
         */
        override fun seek(tupleId: TupleId): Boolean {
            return if (tupleId in 0L..this.header.maxTupleId) {
                this.tupleId = tupleId
                true
            } else {
                false
            }
        }

        /**
         * Returns a boolean indicating whether the entry the the current [FixedHareCursor] position is null.
         *
         * @return true if the entry at the current position of the [FixedHareCursor] is null and false otherwise.
         */
        override fun isNull(): Boolean = this.closeLock.shared {
            check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }
            val page = this.bufferPool.get(this.pageId, Priority.DEFAULT)
            try {
                return (page.getInt(this.entryOffset) and MASK_NULL) > 0L
            } finally {
                page.release()
            }
        }

        /**
         * Returns a boolean indicating whether the entry the the current [FixedHareCursor] position has been deleted.
         *
         * @return true if the entry at the current position of the [FixedHareCursor] has been deleted and false otherwise.
         */
        override fun isDeleted(): Boolean = this.closeLock.shared {
            check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }
            val page = this.bufferPool.get(this.pageId, Priority.DEFAULT)
            try {
                return (page.getInt(this.entryOffset) and MASK_DELETED) > 0L
            } finally {
                page.release()
            }
        }

        /**
         * Returns the entry for the given [TupleId] if such an entry exists.
         *
         * @return Entry [T] for the given [TupleId].
         */
        override fun get(): T? = this.closeLock.shared {
            check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

            val page = this.bufferPool.get(this.pageId, Priority.DEFAULT)
            try {
                val flags = page.getInt(this.entryOffset)
                if ((flags and MASK_DELETED) > 0L) throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be accessed.")
                if ((flags and MASK_NULL) > 0L) {
                    null
                } else {
                    this.serializer.deserialize(page, this.entryOffset + ENTRY_HEADER_SIZE)
                }
            } finally {
                page.release()
            }
        }

        /**
         * Updates the entry for the given [TupleId]
         *
         * @param value The new value [T] the updated entry should contain or null.
         */
        override fun update(value: T?) = this.closeLock.shared {
            check(this.writeable) { "HareCursor has been closed and cannot be used anymore." }
            check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

            /* Check nullability constraint. */
            if (value == null && !this.header.nullable) {
                throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
            }

            val page = this.bufferPool.get(this.pageId, Priority.DEFAULT)
            try {
                val flags = page.getInt(this.entryOffset)
                if ((flags and MASK_DELETED) > 0) throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be updated.")
                if (value != null) {
                    page.putInt(this.entryOffset, (flags and MASK_NULL.inv()))
                    this.serializer.serialize(page, this.entryOffset + ENTRY_HEADER_SIZE, value)
                } else {
                    page.putInt(this.entryOffset, (flags or MASK_NULL))
                    for (i in 0 until this.header.entrySize) {
                        page.putByte(this.entryOffset + ENTRY_HEADER_SIZE + i, 0)
                    }
                }
            } finally {
                page.release()
            }
        }

        /**
         * Updates the entry for the given [TupleId] if it is equal to the expected entry.
         *
         * @param expectedValue The value [T] the entry is expected to contain.
         * @param newValue The new value [T] the updated entry should contain or null.
         */
        override fun compareAndUpdate(expectedValue: T?, newValue: T?): Boolean = this.closeLock.shared {
            check(this.writeable) { "HareCursor is a read-only cursor and cannot be used to write data." }
            check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

            /* Check nullability constraint. */
            if (newValue == null && !this.header.nullable) {
                throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
            }

            val page = this.bufferPool.get(this.pageId, Priority.DEFAULT)
            try {
                val flags = page.getInt(this.entryOffset)
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
                        page.putInt(this.entryOffset, (flags and MASK_NULL.inv()))
                        this.serializer.serialize(page, this.entryOffset + ENTRY_HEADER_SIZE, newValue)
                    } else {
                        page.putInt(this.entryOffset, flags or MASK_NULL)
                        for (i in 0 until this.header.entrySize) {
                            page.putByte(this.entryOffset + ENTRY_HEADER_SIZE + i, 0)
                        }
                    }
                    true
                }
            } finally {
                page.release()
            }
        }

        /**
         * Appends the provided [Value] to the underlying data structure, assigning it a new [TupleId].
         *
         * @param value The [Value] to append. Can be null, if the [FixedHareColumnFile] permits it.
         * @return The [TupleId] of the new value.
         *
         * @throws NullValueNotAllowedException If [value] is null but the underlying data structure does not support null values.
         */
        override fun append(value: T?): TupleId = this.closeLock.shared {
            check(this.writeable) { "HareCursor is a read-only cursor and cannot be used to write data." }
            check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

            /* Check nullability constraint. */
            if (value == null && !this.header.nullable) {
                throw NullValueNotAllowedException("The provided value is null but this HARE column does not support null values.")
            }

            val tupleId = this.header.maxTupleId + 1
            val address = tupleId.toAddress()
            val pageId = address.toPageId()
            val slotId = address.toSlotId()
            val offset = slotId * this.header.entrySize
            if (pageId > this.bufferPool.totalPages) {
                /* Case 1: Data goes on new pages. */
                this.bufferPool.append()
                val page = this.bufferPool.get(pageId)
                try {
                    if (value != null) {
                        page.putLong(offset, 0L)
                        this.serializer.serialize(page, offset + ENTRY_HEADER_SIZE, value)
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
                        this.serializer.serialize(page, offset + ENTRY_HEADER_SIZE, value)
                    } else {
                        page.putInt(offset, MASK_NULL)
                        for (i in 0 until this.header.entrySize) {
                            page.putByte(offset + ENTRY_HEADER_SIZE + i, 0)
                        }
                    }
                } finally {
                    page.release()
                }
            }

            /* Update header. */
            this.header.maxTupleId = tupleId
            this.header.count += 1

            /* Return TupleId. */
            return tupleId
        }

        /**
         * Deletes the entry for the [TupleId] this [WritableCursor] is currently pointing to.
         *
         * @return The value of the entry before deletion.
         *
         * @throws EntryDeletedException If entry identified by [TupleId] has been deleted.
         */
        override fun delete(): T? = this.closeLock.shared {
            check(this.writeable) { "HareCursor is a read-only cursor and cannot be used to write data." }
            check(!this.closed) { "HareCursor has been closed and cannot be used anymore." }

            val page = this.bufferPool.get(this.pageId, Priority.DEFAULT)

            try {
                val flags = page.getInt(this.entryOffset)
                if ((flags and MASK_DELETED) > 0L) throw throw EntryDeletedException("Entry with tuple ID $tupleId has been deleted and cannot be deleted.")

                /* Retrieve current value. */
                val ret = if ((flags and MASK_NULL) > 0L) {
                    null
                } else {
                    this.serializer.deserialize(page, this.entryOffset + ENTRY_HEADER_SIZE)
                }

                /* Delete entry. */
                page.putInt(this.entryOffset, (flags or MASK_DELETED))
                for (i in 0 until this.header.entrySize) {
                    page.putByte(i + ENTRY_HEADER_SIZE + this.entryOffset, 0)
                }

                /* Return deleted value. */
                ret
            } finally {
                page.release()
            }
        }

        /**
         * Closes this [FixedHareCursor] and releases all resources associated with hit.
         */
        override fun close() = this.closeLock.exclusive {
            if (!this.closed) {
                /* Release header page. */
                val headerPage = this.header.page
                if (headerPage is PageRef) {
                    headerPage.release()
                }

                /* Close BufferPool and release lock on column. */
                this.bufferPool.close()
                this@FixedHareColumnFile.closeLock.unlock(this.outerCloseStamp)
                this.closed = true
            }
        }

        /**
         * Inline function: Converts a [TupleId] to an [Address] given the number of slots per [org.vitrivr.cottontail.storage.engine.hare.basics.Page].
         *
         * @return [Address] representation for this [TupleId]
         */
        private fun TupleId.toAddress(): Address = (((this / this@FixedHareColumnFile.slotsPerPage) + 2L) shl 16) or ((this % slotsPerPage) and Short.MAX_VALUE.toLong())
    }
}