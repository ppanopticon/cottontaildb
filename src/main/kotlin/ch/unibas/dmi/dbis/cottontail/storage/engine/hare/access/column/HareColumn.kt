package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.ByteCursor
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.TupleId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.Priority
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Constants
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.read
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import ch.unibas.dmi.dbis.cottontail.utilities.math.BitUtil
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import java.lang.Long.max
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonWritableChannelException
import java.nio.channels.spi.AbstractInterruptibleChannel
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock
/**
 *
 */
class HareColumn <T: Value>(protected val bufferPool: BufferPool, column: ColumnDef<T>? = null) {

    /** Companion object with important constants. */
    companion object {
        /** [PageId] of the root [Page]. */
        const val ROOT_PAGE_ID = 1L

        /** */
        const val BYTE_CURSOR_BOF = 0L

        /** Mask for 'NULLABLE' bit in [HareColumn.Header]. */
        const val MASK_NULLABLE = 1L shl 0

        /** Size of an entry's header in bytes. */
        const val ENTRY_HEADER_SIZE = 8

        /** Mask for 'NULL' bit in each [HareColumn] entry. */
        const val MASK_NULL = 1L shl 0

        /** Mask for 'DELETED' bit in each [HareColumn] entry. */
        const val MASK_DELETED = 1L shl 1

    }

    /** Internal reference to the [Header] of this [HareColumn]. */
    private val header = Header(this.bufferPool.disk.pages == 0L, column)

    /** The [Name] of this [HareColumn]. */
    val name = Name(this.bufferPool.disk.path.fileName.toString().replace(".db", ""))

    /** The [ColumnDef] describing the column managed by this [HareColumn]. */
    val columnDef = ColumnDef(this.name, this.header.type, this.header.size, (this.header.flags and MASK_NULLABLE) != 0L)

    /** The size in bytes of an individual entry in this [HareColumn]. */
    val sizePerEntry = BitUtil.nextPowerOfTwo(this.columnDef.serializer.physicalSize + ENTRY_HEADER_SIZE)

    /** */
    val shiftPerEntry = Integer.numberOfTrailingZeros(this.sizePerEntry)

    /**
     * Creates and returns a [HareCursor] for this [HareColumn]. The [HareCursor] can be used to manipulate the entries in this [HareColumn]
     *
     * @param start The [TupleId] from where to start. Defaults to 1L.
     * @return The [HareCursor] that can be used to alter the given [TupleId]
     */
    fun cursor(start: TupleId = BYTE_CURSOR_BOF, writeable: Boolean = false): HareCursor<T> = HareCursor(this.byteCursor(start, writeable), this.columnDef.serializer)

    /**
     * Creates and returns a [ByteCursor] for this [HareColumn]. The [ByteCursor] can be used to
     * manipulate the raw bytes underlying this [HareColumn]
     *
     * @param start The [TupleId] from where to start
     * @return The [ByteCursor] that can be used to alter the given [TupleId]
     */
    fun byteCursor(start: TupleId = BYTE_CURSOR_BOF, writeable: Boolean = false): ByteCursor = object: AbstractInterruptibleChannel(), ByteCursor {

        /** A [ReentrantReadWriteLock] for access to position. */
        private val tupleIdLock = StampedLock()

        /** A [ReentrantReadWriteLock] for access to position. */
        private val positionLock = StampedLock()

        /** Flag indicating whether this [ByteCursor] is writeable. */
        private val writeable = writeable

        /** [TupleId] this [ByteChannel] is pointing to. */
        private var tupleId: Long = start

        /** Internal position pointer within the entry. Is incremented whenever [ByteCursor.read] and [ByteCursor.write] are called. */
        private var position: Long = -1L
            set(value) {
                field = value
                this.pageId = (value shr Constants.PAGE_BIT_SHIFT) + 2
                this.relativeOffset = (value and (Constants.PAGE_DATA_SIZE_BYTES - 1L)).toInt()
            }


        /** Internal [PageId] pointer to the [Page] that contains the byte at [position]. */
        private var pageId: PageId = -1L
            set(value) {
                if (field != value && value != -1L) {
                    this.page?.release()
                    this.page = this@HareColumn.bufferPool.get(value, Priority.DEFAULT)
                }
                field = value
            }

        private var page: BufferPool.PageRef? = null

        private var relativeOffset = -1

        /** Size of an individual entry in bytes. */
        override val entrySize: Int
            get() = this@HareColumn.sizePerEntry

        /** The maximum [TupleId] supported by this [ByteCursor]. */
        override val maximum: TupleId
            get() = this@HareColumn.header.count

        override fun read(dst: ByteBuffer): Int {
            if (!this.isOpen) { throw ClosedChannelException() }
            this.tupleIdLock.read {
                if (this.tupleId == BYTE_CURSOR_BOF) { throw IndexOutOfBoundsException("Cannot read without moving cursor position beyond BOF.")}
                this.positionLock.write {
                    val toRead = dst.remaining()
                    return if (this.position + toRead<= (this.tupleId shl this@HareColumn.shiftPerEntry)) {
                        this.page!!.getBytes(this.relativeOffset, dst)
                        this.position += toRead
                        toRead
                    } else {
                        0
                    }
                }
            }
        }

        override fun write(src: ByteBuffer): Int {
            if (!this.writeable) { throw NonWritableChannelException() }
            if (!this.isOpen) { throw ClosedChannelException() }
            this.tupleIdLock.read {
                if (this.tupleId == BYTE_CURSOR_BOF) { throw IndexOutOfBoundsException("Cannot read without moving cursor position beyond BOF.")}
                this.positionLock.write {
                    val toWrite = src.remaining()
                    return if (this.position + toWrite <= (this.tupleId shl this@HareColumn.shiftPerEntry)) {
                        this.page!!.putBytes(this.relativeOffset, src)
                        this.position += toWrite
                        toWrite
                    } else {
                        0
                    }
                }
            }
        }

        override fun tupleId(new: TupleId) = this.tupleIdLock.write {
            if (new < BYTE_CURSOR_BOF || new > this.maximum) {
                throw IndexOutOfBoundsException("Given tuple ID $new is out of bounds for this ByteCursor (max = ${this.maximum}).")
            }
            this.tupleId = new
            this.position = (new-1) shl this@HareColumn.shiftPerEntry
        }

        override fun tupleId(): TupleId = this.tupleIdLock.read {
            this.tupleId
        }

        override fun next(): Boolean = try {
            this.tupleId(this.tupleId + 1)
            true
        } catch (e: IndexOutOfBoundsException) {
            false
        }

        override fun previous(): Boolean = try {
            this.tupleId(this.tupleId - 1)
            true
        } catch (e: IndexOutOfBoundsException) {
            false
        }

        override fun append() {
            if (!this.writeable) { throw NonWritableChannelException() }
            if (!this.isOpen) { throw ClosedChannelException() }
            this@HareColumn.header.count += 1
            val minPos = (this.maximum-1) shl this@HareColumn.shiftPerEntry
            val maxPos = this.maximum shl this@HareColumn.shiftPerEntry
            val minPageId = max((minPos shr Constants.PAGE_BIT_SHIFT) + 2, this@HareColumn.bufferPool.disk.pages)
            val maxPageId = max((maxPos shr Constants.PAGE_BIT_SHIFT) + 2, this@HareColumn.bufferPool.disk.pages)
            if (maxPageId > this@HareColumn.bufferPool.disk.pages) {
                for (i in minPageId..maxPageId) {
                    this@HareColumn.bufferPool.append(Priority.DEFAULT).release()
                }
            }
            this.tupleId(this.maximum)
        }

        override fun position(): Long = this.positionLock.read { this.position }

        override fun remaining(): Long = this.tupleIdLock.read {
            (this.tupleId shl this@HareColumn.shiftPerEntry)-this.position
        }

        override fun reset() = this.positionLock.write {
            this.position = (this.tupleId-1) shl this@HareColumn.shiftPerEntry
        }

        override fun implCloseChannel() {
            this.page?.release()
        }
    }


    /**
     * Tries to access the [Page] identified by the given [PageId]. The [Page] will be retrieved with the given [Priority] and, depending
     * on whether read or write access is desired, a corresponding latch on the [Page] will be acquired.
     *
     * @param id The [PageId] of the requested [Page].
     * @param priority The [Priority] with which the requested [Page] should be labeled. High [Priority] [Page]s are more likely to stay in-memory.
     * @param write Whether write access to the [Page] is required.
     * @param action The action that should be executed on the [Page].
     */
    protected inline fun <R> withLock(id: PageId, priority: Priority, action: (BufferPool.PageRef) -> R): R {
        val page = this.bufferPool.get(id, priority)
        val value = action(page)
        page.release()
        return value
    }

    /**
     * The [Header] of this [HareColumn]. The [Header] is located on the first [Page] in the [HareColumn] file.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    protected inner class Header(new: Boolean, column: ColumnDef<T>? = null) {
        init {
            if (new) {
                require(column != null) { }
                this@HareColumn.bufferPool.append(Priority.HIGH)
                withLock(ROOT_PAGE_ID, Priority.HIGH) { page ->
                    page.putInt(0, column.type.ordinal)
                    page.putInt(4, column.size)
                    page.putLong(8, if (column.nullable) { (0L or MASK_NULLABLE) } else { 0L })
                    page.putLong(16, 0L)
                    page.putLong(24, 0L)
                }
            }
        }

        /** The [ColumnType] held by this [HareColumn]. */
        val type: ColumnType<T>
            get() = withLock(ROOT_PAGE_ID, Priority.HIGH) { page ->
            /* TODO: Security measure. */
            ColumnType.forOrdinal(page.getInt(0)) as ColumnType<T>
        }

        /** The logical size of the [ColumnDef] held by this [HareColumn]. */
        val size: Int
            get() = withLock(ROOT_PAGE_ID, Priority.HIGH) { page ->
                page.getInt(4)
            }

        /** Special flags set for this [HareColumn], such as, nullability. */
        val flags: Long
            get() = withLock(ROOT_PAGE_ID, Priority.HIGH) { page ->
                page.getLong(8)
            }

        /** The total number of entries in this [HareColumn]. */
        var count: Long
            get() = withLock(ROOT_PAGE_ID, Priority.HIGH) { page ->
                page.getLong(16)
            }
            set(v) {
                withLock(ROOT_PAGE_ID, Priority.HIGH) { page ->
                    page.putLong(16, v)
                }
            }

        /** The number of deleted entries in this [HareColumn]. */
        var deleted: Long
            get() = withLock(ROOT_PAGE_ID, Priority.HIGH) { page ->
                page.getLong(24)
            }
            set(v) {
                withLock(ROOT_PAGE_ID, Priority.HIGH) { page ->
                    page.putLong(24, v)
                }
            }
    }
}