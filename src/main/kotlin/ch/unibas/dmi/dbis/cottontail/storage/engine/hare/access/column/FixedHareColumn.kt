package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.DataCorruptionException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.ByteCursor
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.TupleId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.Priority
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.*
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.wal.WALDiskManager
import ch.unibas.dmi.dbis.cottontail.utilities.math.BitUtil
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import java.nio.ByteBuffer
import java.nio.file.Path

/**
 *
 */
class FixedHareColumn <T: Value>(val path: Path, wal: Boolean, poolSize: Int = 25) : AutoCloseable {

    /** Companion object with important constants. */
    companion object {

        /** Identifier for HARE_COLUMN_HEADER. *H*are *C*olumn *F*ixed */
        val FILE_HEADER_IDENTIFIER = charArrayOf('H', 'C', 'F')

        /** [PageId] of the root [Page]. */
        const val ROOT_PAGE_ID = 1L

        /** Mask for 'NULLABLE' bit in [FixedHareColumn.Header]. */
        const val MASK_NULLABLE = 1L shl 0

        /** Size of an entry's header in bytes. */
        const val ENTRY_HEADER_SIZE = 8

        /** Mask for 'NULL' bit in each [FixedHareColumn] entry. */
        const val MASK_NULL = 1L shl 0

        /** Mask for 'DELETED' bit in each [FixedHareColumn] entry. */
        const val MASK_DELETED = 1L shl 1

        /**
         * Creates a new [FixedHareColumn] under the given location.
         *
         * @param path [Path] under which to create a new [FixedHareColumn].
         * @param columnDef The [ColumnDef] that describes this [FixedHareColumn].
         */
        fun createDirect(path: Path, columnDef: ColumnDef<*>) {
            DiskManager.create(path)
            val manager = DirectDiskManager(path, 5000)
            val headerPage = Page(ByteBuffer.allocate(Constants.PAGE_DATA_SIZE_BYTES))

            headerPage.putChar(0, FILE_HEADER_IDENTIFIER[0])                                        /* 0: Identifier H. */
            headerPage.putChar(2, FILE_HEADER_IDENTIFIER[1])                                        /* 2: Identifier C. */
            headerPage.putChar(4, FILE_HEADER_IDENTIFIER[2])                                        /* 4: Identifier F. */

            headerPage.putInt(6, columnDef.type.ordinal)                                            /* 6: Type of column. See ColumnDef.forOrdinal() */
            headerPage.putInt(10, columnDef.size)                                                   /* 10: Logical size of column (for structured data types). */
            headerPage.putLong(14, if (columnDef.nullable) { (0L or MASK_NULLABLE) } else { 0L })   /* 14: Column flags; 64 bits, one bit reserved. */
            headerPage.putLong(22, 0L)                                                       /* 22: Number of entries (count) in column. */
            headerPage.putLong(30, 0L)                                                       /* 30: Number of entries (count) in column. */

            manager.allocate(headerPage)
            manager.close()
        }
    }

    /** Initializes the [DiskManager] based on the `wal` property. */
    private val disk = if (wal) { WALDiskManager(this.path) } else { DirectDiskManager(this.path) }

    /** Initializes the [BufferPool]. */
    val bufferPool = BufferPool(disk = this.disk, size = poolSize)

    /** Return true if this [DiskManager] and thus this [FixedHareColumn] is still open. */
    val isOpen
        get() = this.disk.isOpen

    /** The [Name] of this [FixedHareColumn]. */
    val name = Name(this.path.fileName.toString().replace(".db", ""))

    /** The [ColumnDef] describing the column managed by this [FixedHareColumn]. */
    val columnDef = Header().let { header ->
        val def = ColumnDef(this.name, header.type, header.size, (header.flags and MASK_NULLABLE) != 0L)
        def
    }

    /** The size in bytes of an individual entry in this [FixedHareColumn]. Always a power of two! */
    val sizePerEntry = BitUtil.nextPowerOfTwo(this.columnDef.serializer.physicalSize + ENTRY_HEADER_SIZE)

    /**
     * Creates and returns a [HareCursor] for this [FixedHareColumn]. The [HareCursor] can be used to manipulate the entries in this [FixedHareColumn]
     *
     * @param start The [TupleId] from where to start. Defaults to 1L.
     * @return The [HareCursor] that can be used to alter the given [TupleId]
     */
    fun cursor(start: TupleId = HareByteCursor.BYTE_CURSOR_BOF, writeable: Boolean = false): HareCursor<T> = HareCursor(this, writeable, start, this.columnDef.serializer)

    /**
     * Creates and returns a [ByteCursor] for this [FixedHareColumn]. The [ByteCursor] can be used to
     * manipulate the raw bytes underlying this [FixedHareColumn]
     *
     * @param start The [TupleId] from where to start
     * @return The [ByteCursor] that can be used to alter the given [TupleId]
     */
    fun byteCursor(start: TupleId = HareByteCursor.BYTE_CURSOR_BOF, writeable: Boolean = false): ByteCursor = HareByteCursor(this, writeable, start)

    /**
     * Closes this [FixedHareColumn]. Closing a [FixedHareColumn] invalidates all resources such
     * as [HareByteCursor], [HareCursor] and [FixedHareColumn.Header]. Using these structures after
     * closing the paren [FixedHareColumn] is a programmer's error!
     */
    override fun close() {
        if (!this.disk.isOpen) {
            this.disk.close()
        }
    }

    /**
     * The [Header] of this [FixedHareColumn]. The [Header] is located on the first [Page] in the [FixedHareColumn] file.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class Header {
        /** [BufferPool.PageRef] used by this [Header]. This [BufferPool.PageRef] remains locked until finalization. */
        private val page: BufferPool.PageRef = this@FixedHareColumn.bufferPool.get(ROOT_PAGE_ID, Priority.HIGH)

        /** Make necessary check on initialization. */
        init {
            require(this.page.getChar(0) == FILE_HEADER_IDENTIFIER[0]) { DataCorruptionException("Identifier mismatch in HARE fixed column file ${this@FixedHareColumn.path.fileName}.") }
            require(this.page.getChar(2) == FILE_HEADER_IDENTIFIER[1]) { DataCorruptionException("Identifier mismatch in HARE fixed column file ${this@FixedHareColumn.path.fileName}.") }
            require(this.page.getChar(4) == FILE_HEADER_IDENTIFIER[2]) { DataCorruptionException("Identifier mismatch in HARE fixed column file ${this@FixedHareColumn.path.fileName}.") }

            try {
                ColumnType.forOrdinal(this.page.getInt(6))
            } catch (e: IllegalArgumentException) {
                throw DataCorruptionException("Column type ordinal mismatch in HARE fixed column file ${this@FixedHareColumn.path.fileName}.")
            }
            require(this.page.getLong(22) >= 0) { DataCorruptionException("Negative number of entries in HARE fixed column file ${this@FixedHareColumn.path.fileName}.") }
            require(this.page.getLong(30) >= 0) { DataCorruptionException("Negative number of deleted entries in HARE fixed column file ${this@FixedHareColumn.path.fileName}.") }
        }

        /** The [ColumnType] held by this [FixedHareColumn]. */
        @Suppress("UNCHECKED_CAST")
        val type: ColumnType<T>
            get() {
                return ColumnType.forOrdinal(this.page.getInt(6)) as ColumnType<T>
            }

        /** The logical size of the [ColumnDef] held by this [FixedHareColumn]. */
        val size: Int
            get() {
                return this.page.getInt(10)
            }

        /** Special flags set for this [FixedHareColumn], such as, nullability. */
        val flags: Long
            get() {
                return this.page.getLong(14)
            }

        /** The total number of entries in this [FixedHareColumn]. */
        var count: Long
            get() {
                return this.page.getLong(22)
            }
            set(v) {
                this.page.putLong(22, v)
            }

        /** The number of deleted entries in this [FixedHareColumn]. */
        var deleted: Long
            get() {
                return this.page.getLong(30)
            }
            set(v) {
                this.page.putLong(30, v)
            }
    }
}