package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile.FixedHareCursor
import org.vitrivr.cottontail.storage.engine.hare.access.cursor.Cursor
import org.vitrivr.cottontail.storage.engine.hare.access.cursor.ReadableCursor
import org.vitrivr.cottontail.storage.engine.hare.access.cursor.WritableCursor
import org.vitrivr.cottontail.storage.engine.hare.addressFor
import org.vitrivr.cottontail.storage.engine.hare.basics.PageRef
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionPolicy
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.DataPage
import org.vitrivr.cottontail.storage.engine.hare.disk.wal.WALDiskManager
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer
import org.vitrivr.cottontail.storage.engine.hare.toPageId
import org.vitrivr.cottontail.storage.engine.hare.toSlotId
import org.vitrivr.cottontail.storage.engine.hare.views.*
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.shared
import org.vitrivr.cottontail.utilities.math.BitUtil
import java.lang.Long.max
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.locks.StampedLock

/**
 * A HARE column file where each entry has a fixed size and can be addressed by a [TupleId].
 *
 * @author Ralph Gasser
 * @param 1.0.1
 */
class VariableHareColumnFile<T : Value>(val path: Path, wal: Boolean, corePoolSize: Int = 5) : AutoCloseable {
    /** Companion object with important constants. */
    companion object {

        /** [PageId] of the root [DataPage] in a [VariableHareColumnFile. */
        const val ROOT_PAGE_ID: PageId = 1L

        /** [PageId] of the root [DirectoryPageView]. */
        const val ROOT_DIRECTORY_PAGE_ID: PageId = 2L

        /** [PageId] of the root [SlottedPageView]. */
        const val ROOT_ALLOCATION_PAGE_ID: PageId = 3L

        /**
         * Creates a new [VariableHareColumnFile] under the given location.
         *
         * @param path [Path] to the directory in which to create the [VariableHareColumnFile].
         * @param columnDef The [ColumnDef] that describes this [VariableHareColumnFile].
         */
        fun create(path: Path, columnDef: ColumnDef<*>) {
            val entrySize = columnDef.serializer.physicalSize + SlottedPageView.SIZE_ENTRY /* Each entry has a offset entry on the slotted page. */
            val pageShift = determinePageSize(entrySize)
            DiskManager.create(path, pageShift)

            val manager = DirectDiskManager(path, 5000)

            /** Allocate file header page. */
            val page = DataPage(ByteBuffer.allocate(manager.pageSize))
            HeaderPageView().initializeAndWrap(page, columnDef)
            manager.update(manager.allocate(), page)

            /** Allocate first directory page. */
            DirectoryPageView().initializeAndWrap(page.clear(), DirectoryPageView.NO_REF, 0L)
            manager.update(manager.allocate(), page)

            /** Allocate first slotted page. */
            SlottedPageView().initializeAndWrap(page.clear())
            manager.update(manager.allocate(), page)

            /** Close manager. */
            manager.close()
        }

        /**
         * Determines the optimal size of page based on the size of an entry and the desired fill factor.
         *
         * @param entrySize Size of an entry in bytes.
         */
        private fun determinePageSize(entrySize: Int): Int {
            var pageShift = StrictMath.max(BitUtil.toShift(BitUtil.nextPowerOfTwo(entrySize)), DiskManager.MIN_PAGE_SHIFT)
            var waste = (1 shl pageShift) - SlottedPageView.SIZE_HEADER - entrySize * StrictMath.floorDiv((1 shl pageShift), entrySize)
            for (i in (pageShift + 1)..DiskManager.MAX_PAGE_SHIFT) {
                val newWaste = (1 shl i) - SlottedPageView.SIZE_HEADER - entrySize * StrictMath.floorDiv((1 shl i), entrySize)
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

    /** The [Name] of this [VariableHareColumnFile]. */
    val name = Name.ColumnName(this.path.fileName.toString().replace(".db", ""))

    /** The [ColumnDef] describing the column managed by this [VariableHareColumnFile]. */
    val columnDef: ColumnDef<T>

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
        page.release()
    }

    /**
     * Creates and returns a [ReadableCursor] for this [VariableHareColumnFile]. The [ReadableCursor] can be
     * used to access the entries in this [VariableHareColumnFile]
     *
     * @param bufferSize The size of the [BufferPool] that backs the new [Cursor].
     *
     * @return The [WritableCursor] that can be used to alter the given [TupleId]
     */
    fun cursor(bufferSize: Int = 10): ReadableCursor<T> = VariableHareCursor(false, bufferSize)

    /**
     * Creates and returns a [WritableCursor] for this [VariableHareColumnFile]. The [WritableCursor] can be
     * used to access and manipulate the entries in this [VariableHareColumnFile]
     *
     * @param bufferSize The size of the [BufferPool] that backs the new [Cursor].
     *
     * @return The [WritableCursor] that can be used to alter the given [TupleId]
     */
    fun writableCursor(bufferSize: Int = 10): WritableCursor<T> = VariableHareCursor(true, bufferSize)

    /**
     * Closes this [VariableHareColumnFile].
     */
    override fun close() = this.closeLock.exclusive {
        if (!this.disk.isOpen) {
            this.bufferPool.close()
            this.disk.close()
            this.closed = true
        }
    }

    /**
     * A [Cursor] for access to the raw entries in a [VariableHareColumnFile].
     *
     * @author Ralph Gasser
     * @version 1.0.1
     */
    inner class VariableHareCursor(val writeable: Boolean, val bufferSize: Int) : WritableCursor<T>, ReadableCursor<T> {
        /** Internal [BufferPool]; shared for writeable cursors and dedicated for read-only cursors. */
        private val bufferPool = if (writeable) {
            this@VariableHareColumnFile.bufferPool
        } else {
            BufferPool(this@VariableHareColumnFile.disk, this.bufferSize, EvictionPolicy.FIFO)
        }

        /** Local reference to the [Serializer] used for this [VariableHareCursor]. */
        private val serializer = this@VariableHareColumnFile.columnDef.serializer

        /** The [DirectoryCursor] object used by this [VariableHareCursor]. */
        private val directory = DirectoryCursor(this.bufferPool)

        /** The [HeaderPageView] used by this [DirectoryCursor]. */
        private val headerView = HeaderPageView().wrap(this.bufferPool.get(ROOT_PAGE_ID, Priority.HIGH))

        /** The [DirectoryCursor] object used by this [VariableHareCursor]. */
        private val slottedPageView = SlottedPageView()

        /** Acquires a latch on the outer [VariableHareColumnFile]. This latch remains active until [VariableHareCursor] is released. */
        private val outerCloseStamp = this@VariableHareColumnFile.closeLock.readLock()

        /** The maximum [TupleId] supported by this [VariableHareCursor]. */
        override val maximum: TupleId
            get() {
                this.directory.last()
                return this.directory.lastTupleId
            }

        /** Internal flag used to indicate, that this [FixedHareCursor] was closed. */
        @Volatile
        private var closed: Boolean = false

        /** Internal lock to mediate access to closing the [VariableHareCursor]. */
        private val closeLock = StampedLock()

        /** */
        override var tupleId: TupleId = -1L
            private set

        /**
         * Moves this [VariableHareCursor] to the next [TupleId].
         *
         * @return True, if [VariableHareCursor] has been moved, false otherwise.
         */
        override fun next(): Boolean = this.closeLock.shared {
            check(!this.closed) { "Cursor has been closed and cannot be used anymore." }
            val newTupleId = this.tupleId + 1
            if (this.directory.has(newTupleId)) {
                this.tupleId = newTupleId
                return true
            } else if (this.directory.next() && this.directory.has(newTupleId)) {
                this.tupleId = newTupleId
                return true
            }
            return false
        }

        /**
         * Moves this [VariableHareCursor] to the previous [TupleId].
         *
         * @return True, if [VariableHareCursor] has been moved, false otherwise.
         */
        override fun previous(): Boolean = this.closeLock.shared {
            check(!this.closed) { "Cursor has been closed and cannot be used anymore." }
            val newTupleId = this.tupleId - 1
            if (this.directory.has(newTupleId)) {
                this.tupleId = newTupleId
                return true
            } else if (this.directory.previous() && this.directory.has(newTupleId)) {
                this.tupleId = newTupleId
                return true
            }
            return false
        }

        /**
         * Seeks the given [TupleId] and moves this [VariableHareCursor]'s position to it.
         *
         * @return True, if [VariableHareCursor] has been moved, false otherwise.
         */
        override fun seek(tupleId: TupleId): Boolean = this.closeLock.shared {
            check(!this.closed) { "Cursor has been closed and cannot be used anymore." }
            if (this.directory.has(tupleId)) {
                this.tupleId = tupleId
                return true
            }
            while (true) {
                if (this.tupleId < this.directory.firstTupleId && this.directory.previous()) {
                    if (this.directory.has(tupleId)) {
                        return true
                    }
                } else if (this.tupleId > this.directory.lastTupleId && this.directory.next()) {
                    if (this.directory.has(tupleId)) {
                        return true
                    }
                } else {
                    return false
                }
            }
            false
        }

        /**
         * Returns a boolean indicating whether the entry this [VariableHareCursor] is currently pointing to is null.
         *
         * @return true if the entry for the given [TupleId] is null and false otherwise.
         */
        override fun isNull(): Boolean = this.closeLock.shared {
            this.directory.getFlags(this.tupleId).isNull()
        }

        /**
         * Returns a boolean indicating whether the entry this [VariableHareCursor] is currently pointing to has been deleted.
         *
         * @return true if the entry for the given [TupleId] is null and false otherwise.
         */
        override fun isDeleted(): Boolean = this.closeLock.shared {
            this.directory.getFlags(this.tupleId).isDeleted()
        }

        override fun get(): T? = this.closeLock.shared {
            val address = this.directory.getAddress(this.tupleId)
            val page = this.bufferPool.get(address.toPageId())
            val view = this.slottedPageView.wrap(page)
            val offset = view.offset(address.toSlotId())
            val value = this.serializer.deserialize(page, offset)
            page.release()
            return value
        }

        override fun update(value: T?) {
            TODO("Not yet implemented")
        }

        override fun compareAndUpdate(expectedValue: T?, newValue: T?): Boolean {
            TODO("Not yet implemented")
        }

        override fun delete(): T? {
            TODO("Not yet implemented")
        }

        /**
         *
         */
        override fun append(value: T?): TupleId = this.closeLock.shared {

            /** Try to allocate data on allocation page. */
            val allocationSize = if (value == null) {
                2
            } else {
                this.serializer.physicalSize
            }
            var allocationPageId = this.headerView.allocationPageId
            var allocationPage = this.bufferPool.get(this.headerView.allocationPageId, Priority.LOW)
            val view = SlottedPageView().wrap(allocationPage)
            var slotId = view.allocate(allocationSize) // TODO: Make dynamic

            /** If allocation page is full, create new one and store data there */
            if (slotId == null) {
                allocationPage.release()
                allocationPageId = max(this.headerView.allocationPageId, this.headerView.lastDirectoryPageId) + 1L
                if (allocationPageId >= this.bufferPool.totalPages) {
                    allocationPageId = this.bufferPool.append()
                }
                allocationPage = this.bufferPool.get(allocationPageId)
                slotId = view.initializeAndWrap(allocationPage).allocate(allocationSize) ?: TODO("Data that does not fit a single page.")
                this.headerView.allocationPageId = allocationPageId
            }

            /** Generate address and tupleId. */
            val address = addressFor(allocationPageId, slotId)
            val flags = if (value == null) {
                (0 or VARIABLE_FLAGS_MASK_NULL)
            } else {
                0
            }
            val newTupleId = this.directory.newTupleId(flags, address)

            /* Write data and release pages. */
            if (value != null) {
                this.serializer.serialize(allocationPage, view.offset(slotId), value)
            }
            allocationPage.release()

            newTupleId
        }

        /**
         * Closes this [VariableHareCursor] and releases all resources associated with hit.
         */
        override fun close() = this.closeLock.exclusive {
            if (!this.closed) {
                /* Release header page. */
                val headerPage = this.headerView.page
                if (headerPage is PageRef) {
                    headerPage.release()
                }

                this.directory.close()
                this.bufferPool.close()
                this@VariableHareColumnFile.closeLock.unlock(this.outerCloseStamp)
                this.closed = true
            }
        }
    }
}