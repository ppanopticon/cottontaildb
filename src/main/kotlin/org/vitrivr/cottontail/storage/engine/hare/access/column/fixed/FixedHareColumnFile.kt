package org.vitrivr.cottontail.storage.engine.hare.access.column.fixed

import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.*
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.*
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.Priority
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionPolicy
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.disk.wal.WALHareDiskManager
import org.vitrivr.cottontail.utilities.extensions.exclusive
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
class FixedHareColumnFile <T: Value>(val path: Path, wal: Boolean, corePoolSize: Int = 5) : HareColumnFile<T> {

    /** Companion object with important constants. */
    companion object {
        /** [PageId] of the root [HarePage]. */
        const val ROOT_PAGE_ID: PageId = 1L

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
            HareDiskManager.create(path, pageShift)

            val manager = DirectHareDiskManager(path, 5000)

            /** Allocate file header page. */
            val page = HarePage(ByteBuffer.allocate(manager.pageSize))
            HeaderPageView.initialize(page, columnDef)

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
            var pageShift = max(BitUtil.toShift(BitUtil.nextPowerOfTwo(entrySize)), HareDiskManager.MIN_PAGE_SHIFT)
            var waste = (1 shl pageShift) - entrySize * floorDiv((1 shl pageShift), entrySize)
            for (i in (pageShift+1)..HareDiskManager.MAX_PAGE_SHIFT) {
                val newWaste = (1 shl i) - entrySize * floorDiv((1 shl i), entrySize)
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

    /** The [Name] of this [FixedHareColumnFile]. */
    val name = Name.ColumnName(this.path.fileName.toString().replace(".db", ""))

    /** The [ColumnDef] describing the column managed by this [FixedHareColumnFile]. */
    val columnDef: ColumnDef<T>

    /** The size of an entry in bytes. */
    val entrySize: Int

    /** The number of slots per page. */
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
        this.entrySize = header.entrySize
        this.slotsPerPage = floorDiv(1 shl this@FixedHareColumnFile.disk.pageShift, (header.entrySize + ENTRY_HEADER_SIZE))
        page.release()
    }

    /**
     * Creates a new [FixedHareColumnReader] and returns it.
     *
     * @return [FixedHareColumnReader]
     */
    override fun newReader() = FixedHareColumnReader(this)

    /**
     * Creates a new [FixedHareColumnWriter] and returns it.
     *
     * @return [FixedHareColumnWriter]
     */
    override fun newWriter() = FixedHareColumnWriter(this)

    /**
     * Creates a new [FixedHareColumnCursor] and returns it.
     *
     * @return [FixedHareColumnWriter]
     */
    override fun newCursor() = FixedHareColumnCursor(this)

    /**
     * Converts a [TupleId] to an [Address] given the number of slots per [org.vitrivr.cottontail.storage.engine.hare.basics.Page].
     *
     * @return [Address] representation for this [TupleId]
     */
    fun toAddress(tupleId: TupleId): Address = (((tupleId / this@FixedHareColumnFile.slotsPerPage) + 2L) shl 16) or ((tupleId % slotsPerPage) and Short.MAX_VALUE.toLong())

    /**
     * Closes this [FixedHareColumnFile].
     */
    override fun close() = this.closeLock.exclusive {
        if (!this.closed) {
            this.bufferPool.close()
            this.disk.close()
            this.closed = true
        }
    }
}