package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnReader
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareCursor
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionPolicy
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.disk.wal.WALHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.views.*
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.math.BitUtil

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
            HeaderPageView.initialize(page, columnDef)
            manager.update(manager.allocate(), page)

            /** Allocate first directory page. */
            DirectoryPageView.initialize(page.clear(), DirectoryPageView.NO_REF, 0L)
            manager.update(manager.allocate(), page)

            /** Allocate first slotted page. */
            SlottedPageView.initialize(page.clear())
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
}