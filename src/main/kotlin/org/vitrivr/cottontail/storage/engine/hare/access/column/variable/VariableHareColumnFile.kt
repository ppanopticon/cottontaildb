package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile.FixedHareCursor
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnReader
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareCursor
import org.vitrivr.cottontail.storage.engine.hare.addressFor
import org.vitrivr.cottontail.storage.engine.hare.basics.PageRef
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionPolicy
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.disk.wal.WALHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer
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
class VariableHareColumnFile<T : Value>(val path: Path, wal: Boolean, corePoolSize: Int = 5) : HareColumnFile<T> {
    /** Companion object with important constants. */
    companion object {

        /** [PageId] of the root [HarePage] in a [VariableHareColumnFile. */
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
            HareDiskManager.create(path, pageShift)

            val manager = DirectHareDiskManager(path, 5000)

            /** Allocate file header page. */
            val page = HarePage(ByteBuffer.allocate(manager.pageSize))
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
            var pageShift = StrictMath.max(BitUtil.toShift(BitUtil.nextPowerOfTwo(entrySize)), HareDiskManager.MIN_PAGE_SHIFT)
            var waste = (1 shl pageShift) - SlottedPageView.SIZE_HEADER - entrySize * StrictMath.floorDiv((1 shl pageShift), entrySize)
            for (i in (pageShift + 1)..HareDiskManager.MAX_PAGE_SHIFT) {
                val newWaste = (1 shl i) - SlottedPageView.SIZE_HEADER - entrySize * StrictMath.floorDiv((1 shl i), entrySize)
                if (newWaste < waste) {
                    waste = newWaste
                    pageShift = i
                }
            }
            return pageShift
        }
    }

    /** Initializes the [HareDiskManager] based on the `wal` property. */
    val disk = if (wal) {
        WALHareDiskManager(this.path)
    } else {
        DirectHareDiskManager(this.path)
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

    override fun newReader(): HareColumnReader<T> = VariableHareColumnReader(this, Directory(this))

    override fun newWriter(): HareColumnWriter<T> {
        TODO("Not yet implemented")
    }

    override fun newCursor(): HareCursor<T> = VariableHareColumnCursor(this, Directory(this))

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
     * A [HareCursor] for access to the raw entries in a [VariableHareColumnFile].
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

        /** The [Directory] object used by this [VariableHareCursor]. */
        private val directoryView = Directory(this.bufferPool)

        /** The [HeaderPageView] used by this [Directory]. */
        private val headerView = HeaderPageView().wrap(this.bufferPool.get(ROOT_PAGE_ID, Priority.HIGH))

        /** The [Directory] object used by this [VariableHareCursor]. */
        private val slottedPageView = SlottedPageView()

        /** Acquires a latch on the outer [VariableHareColumnFile]. This latch remains active until [VariableHareCursor] is released. */
        private val outerCloseStamp = this@VariableHareColumnFile.closeLock.readLock()

        /** The maximum [TupleId] supported by this [VariableHareCursor]. */
        override val maximum: TupleId
            get() {
                this.directoryView.last()
                return this.directoryView.lastTupleId
            }

        /** The number of elements addressed by this [FixedHareCursor]. */
        override val size: Long
            get() = this.headerView.count

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
            if (this.directoryView.has(newTupleId)) {
                this.tupleId = newTupleId
                return true
            } else if (this.directoryView.next() && this.directoryView.has(newTupleId)) {
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
            if (this.directoryView.has(newTupleId)) {
                this.tupleId = newTupleId
                return true
            } else if (this.directoryView.previous() && this.directoryView.has(newTupleId)) {
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
            if (this.directoryView.has(tupleId)) {
                this.tupleId = tupleId
                return true
            }
            while (true) {
                if (this.tupleId < this.directoryView.firstTupleId && this.directoryView.previous()) {
                    if (this.directoryView.has(tupleId)) {
                        return true
                    }
                } else if (this.tupleId > this.directoryView.lastTupleId && this.directoryView.next()) {
                    if (this.directoryView.has(tupleId)) {
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
            this.directoryView.getFlags(this.tupleId).isNull()
        }

        /**
         * Returns a boolean indicating whether the entry this [VariableHareCursor] is currently pointing to has been deleted.
         *
         * @return true if the entry for the given [TupleId] is null and false otherwise.
         */
        override fun isDeleted(): Boolean = this.closeLock.shared {
            this.directoryView.getFlags(this.tupleId).isDeleted()
        }

        override fun get(): T? = this.closeLock.shared {
            val address = this.directoryView.getAddress(this.tupleId)
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
            val newTupleId = this.directoryView.newTupleId(flags, address)

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

                this.directoryView.close()
                this.bufferPool.close()
                this@VariableHareColumnFile.closeLock.unlock(this.outerCloseStamp)
                this.closed = true
            }
        }
    }
}