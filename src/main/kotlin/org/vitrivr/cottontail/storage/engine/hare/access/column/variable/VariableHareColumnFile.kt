package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.access.column.directory.DirectoryPageView
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.disk.wal.WALHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.views.SlottedPageView
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
class VariableHareColumnFile<T : Value>(val path: Path, wal: Boolean) : HareColumnFile<T> {
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
            val tid = -1L

            /** Allocate file header page. */
            val page = HarePage(ByteBuffer.allocate(manager.pageSize))
            HeaderPageView.initialize(page, columnDef)
            manager.update(tid, manager.allocate(tid), page)

            /** Allocate first directory page. */
            DirectoryPageView.initialize(page.clear(), DirectoryPageView.NO_REF, 0L)
            manager.update(tid, manager.allocate(tid), page)

            /** Allocate first slotted page. */
            SlottedPageView.initialize(page.clear())
            manager.update(tid, manager.allocate(tid), page)

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
    override val disk = WALHareDiskManager(this.path)

    /** The [Name] of this [VariableHareColumnFile]. */
    override val name: String
        get() = this.path.fileName.toString().replace(".${HareColumnFile.SUFFIX}", "")

    /** The [ColumnType] describing the column managed by this [VariableHareColumnFile]. */
    override val columnType: ColumnType<T>

    /** The logical size of the values contained in this [VariableHareColumnFile]. */
    override val logicalSize: Int

    /** Flag indicating whether this [VariableHareColumnFile] supports null entries or not. */
    override val nullable: Boolean

    /** Flag indicating, whether this [VariableHareColumnFile] is still open and safe for use. */
    override val isOpen: Boolean
        get() = this.disk.isOpen

    /** A [StampedLock] used to prevent this [VariableHareColumnFile] from closing, when it is being used by other resources. */
    private val closeLock = StampedLock()

    /* Initialize important fields. */
    init {
        val page = HarePage(ByteBuffer.allocate(this.disk.pageSize))
        val tid = -1L

        this.disk.read(tid, FixedHareColumnFile.ROOT_PAGE_ID, page)
        val header = HeaderPageView(page).validate()
        this.columnType = header.type as ColumnType<T>
        this.logicalSize = header.size
        this.nullable = header.nullable
    }

    /**
     * Closes this [VariableHareColumnFile].
     */
    override fun close() = this.closeLock.exclusive {
        if (!this.disk.isOpen) {
            this.disk.close()
        }
    }

    /**
     * Tries to obtain a close lock on this [VariableHareColumnFile].
     *
     * @return Close lock handle
     */
    override fun obtainLock(): Long = this.closeLock.readLock()

    /**
     * Releases the given close lock handle.
     *
     * @param handle Close lock handle to release.
     */
    override fun releaseLock(handle: Long) {
        this.closeLock.unlockRead(handle)
    }
}