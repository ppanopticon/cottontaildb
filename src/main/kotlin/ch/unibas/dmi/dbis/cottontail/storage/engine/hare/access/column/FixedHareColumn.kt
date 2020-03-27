package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.DataCorruptionException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column.AbstractCursor.Companion.BYTE_CURSOR_BOF
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.TupleId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.PageRef
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.Priority
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DataPage
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.wal.WALDiskManager
import ch.unibas.dmi.dbis.cottontail.utilities.math.BitUtil
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import java.lang.StrictMath.floorDiv
import java.lang.StrictMath.max
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.math.abs

/**
 * A HARE column file where each entry has a fixed size and can be addressed by a [TupleId].
 *
 * @author Ralph Gasser
 * @param 1.0
 */
class FixedHareColumn <T: Value>(val path: Path, wal: Boolean, poolSize: Int = 25) : AutoCloseable {

    /** Companion object with important constants. */
    companion object {

        /** Identifier for HARE_COLUMN_HEADER. *H*are *C*olumn *F*ixed */
        val FILE_HEADER_IDENTIFIER = charArrayOf('H', 'C', 'F')

        /** [PageId] of the root [DataPage]. */
        const val ROOT_PAGE_ID = 0L

        /** Mask for 'NULLABLE' bit in [FixedHareColumn.Header]. */
        const val MASK_NULLABLE = 1L shl 0

        /** Size of an entry's header in bytes. */
        const val ENTRY_HEADER_SIZE = 8

        /** Mask for 'NULL' bit in each [FixedHareColumn] entry. */
        const val MASK_NULL = 1L shl 1

        /** Mask for 'DELETED' bit in each [FixedHareColumn] entry. */
        const val MASK_DELETED = 1L shl 2

        /**
         * Creates a new [FixedHareColumn] under the given location.
         *
         * @param path [Path] under which to create a new [FixedHareColumn].
         * @param columnDef The [ColumnDef] that describes this [FixedHareColumn].
         * @param desiredFillFactor The desired number of entries per page (fill factor = (entrySize / pageSize)). Used to determine the size of a page.
         */
        fun createDirect(path: Path, columnDef: ColumnDef<*>, desiredFillFactor: Int = 8) {
            val entrySize = columnDef.serializer.physicalSize + ENTRY_HEADER_SIZE
            val pageShift = determinePageSize(entrySize, desiredFillFactor)
            DiskManager.create(path, pageShift)
            val manager = DirectDiskManager(path, 5000)
            val headerPage = DataPage(ByteBuffer.allocate(manager.pageSize))

            headerPage.putChar(0, FILE_HEADER_IDENTIFIER[0])                                        /* 0: Identifier H. */
            headerPage.putChar(2, FILE_HEADER_IDENTIFIER[1])                                        /* 2: Identifier C. */
            headerPage.putChar(4, FILE_HEADER_IDENTIFIER[2])                                        /* 4: Identifier F. */

            headerPage.putInt(6, columnDef.type.ordinal)                                            /* 6: Type of column. See ColumnDef.forOrdinal() */
            headerPage.putInt(10, columnDef.size)                                                   /* 10: Logical size of column (for structured data types). */
            headerPage.putInt(14, entrySize)                                                        /* 14: Physical size of a column in bytes. */

            headerPage.putLong(18, if (columnDef.nullable) { (0L or MASK_NULLABLE) } else { 0L })   /* 18: Column flags; 64 bits, one bit reserved. */
            headerPage.putLong(22, 0L)                                                       /* 26: Number of entries (count) in column. */
            headerPage.putLong(30, 0L)                                                       /* 34: Number of deleted entries (count) in column. */

            manager.allocate(headerPage)
            manager.close()
        }

        /**
         * Determines the optimal size of page based on the size of an entry and the desired fill factor.
         *
         * @param entrySize Size of an entry in bytes.
         * @param desiredFillFactor Desired fill factor (i.e., entries per page)
         */
        private fun determinePageSize(entrySize: Int, desiredFillFactor: Int): Int {
            val pageShift = Integer.numberOfTrailingZeros(BitUtil.nextPowerOfTwo(entrySize))
            var fillFactor = floorDiv((1 shl pageShift), entrySize)
            var delta = abs(fillFactor - desiredFillFactor)
            for (i in (max(pageShift, DiskManager.MIN_PAGE_SHIFT)+1)..DiskManager.MAX_PAGE_SHIFT) {
                fillFactor = floorDiv((1 shl i), entrySize)
                if (abs(fillFactor - desiredFillFactor) <= delta) {
                    delta = abs(fillFactor - desiredFillFactor)
                } else {
                    return i-1
                }
            }
            return pageShift
        }
    }

    /** Initializes the [DiskManager] based on the `wal` property. */
    val disk = if (wal) { WALDiskManager(this.path) } else { DirectDiskManager(this.path) }

    /** Initializes the [BufferPool]. */
    val bufferPool = BufferPool(disk = this.disk, size = poolSize)

    /** Return true if this [DiskManager] and thus this [FixedHareColumn] is still open. */
    val isOpen
        get() = this.disk.isOpen

    /** The [Name] of this [FixedHareColumn]. */
    val name = Name(this.path.fileName.toString().replace(".db", ""))

    /** The private instance of the [FixedHareColumn] file header. */
    private val header = Header()

    /** The [ColumnDef] describing the column managed by this [FixedHareColumn]. */
    val columnDef = ColumnDef(this.name, this.header.type, this.header.size, (this.header.flags and MASK_NULLABLE) != 0L)

    /** The physical size of an individual entry or tuple in this [FixedHareColumn]. */
    val entrySize = this.header.entrySize

    /**
     * Creates and returns a [BufferedHareCursor] for this [FixedHareColumn]. The [BufferedHareCursor] can be used to manipulate the entries in this [FixedHareColumn]
     *
     * @param start The [TupleId] from where to start. Defaults to 1L.
     * @return The [BufferedHareCursor] that can be used to alter the given [TupleId]
     */
    fun cursor(start: TupleId = BYTE_CURSOR_BOF, writeable: Boolean = false): BufferedHareCursor<T> = BufferedHareCursor(this, writeable, start, this.columnDef.serializer)

    /**
     * Closes this [FixedHareColumn]. Closing a [FixedHareColumn] invalidates all resources such [BufferedHareCursor]
     * and [FixedHareColumn.Header]. Using these structures after closing the paren [FixedHareColumn] is a programmer's error!
     */
    override fun close() {
        if (!this.disk.isOpen) {
            this.disk.close()
        }
    }

    /**
     * The [Header] of this [FixedHareColumn]. The [Header] is located on the first [DataPage] in the [FixedHareColumn] file.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class Header {
        /** [BufferPool.PageReference] used by this [Header]. This [BufferPool.PageReference] remains locked until finalization. */
        private val page: PageRef = this@FixedHareColumn.bufferPool.get(ROOT_PAGE_ID, Priority.HIGH)

        /** Make necessary check on initialization. */
        init {
            require(this.page.getChar(0) == FILE_HEADER_IDENTIFIER[0]) { DataCorruptionException("Identifier mismatch in HARE fixed column file (file: ${this@FixedHareColumn.path.fileName}).") }
            require(this.page.getChar(2) == FILE_HEADER_IDENTIFIER[1]) { DataCorruptionException("Identifier mismatch in HARE fixed column file (file: ${this@FixedHareColumn.path.fileName}).") }
            require(this.page.getChar(4) == FILE_HEADER_IDENTIFIER[2]) { DataCorruptionException("Identifier mismatch in HARE fixed column file (file: ${this@FixedHareColumn.path.fileName}).") }
            try {
                ColumnType.forOrdinal(this.page.getInt(6))
            } catch (e: IllegalArgumentException) {
                throw DataCorruptionException("Column type ordinal mismatch in HARE fixed column file ${this@FixedHareColumn.path.fileName}.")
            }
            require(this.page.getInt(14) <= (1 shl this@FixedHareColumn.disk.pageShift)) { DataCorruptionException("Entry size mismatch in HARE fixed column file; entry size must be smaller or equal to page size ${this@FixedHareColumn.path.fileName}.") }
            require(this.page.getLong(22) >= 0) { DataCorruptionException("Negative number of entries in HARE fixed column file ${this@FixedHareColumn.path.fileName}.") }
            require(this.page.getLong(30) >= 0) { DataCorruptionException("Negative number of deleted entries in HARE fixed column file ${this@FixedHareColumn.path.fileName}.") }
        }

        /** The [ColumnType] held by this [FixedHareColumn]. */
        @Suppress("UNCHECKED_CAST")
        val type: ColumnType<T>
            get() = ColumnType.forOrdinal(this.page.getInt(6)) as ColumnType<T>

        /** The logical size of the [ColumnDef] held by this [FixedHareColumn]. */
        val size: Int
            get() = this.page.getInt(10)

        /** The size of an entry in bytes. */
        val entrySize: Int
            get() = this.page.getInt(14)

        /** Special flags set for this [FixedHareColumn], such as, nullability. */
        val flags: Long
            get() = this.page.getLong(18)

        /** The total number of entries in this [FixedHareColumn]. */
        var count: Long
            get() = this.page.getLong(26)
            set(v) {
                this.page.putLong(26, v)
            }

        /** The number of deleted entries in this [FixedHareColumn]. */
        var deleted: Long
            get() = this.page.getLong(34)
            set(v) {
                this.page.putLong(34, v)
            }
    }
}