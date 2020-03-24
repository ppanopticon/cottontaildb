package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.ByteCursor
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.TupleId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.Priority
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Constants
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonWritableChannelException
import java.nio.channels.spi.AbstractInterruptibleChannel
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * A [ByteCursor] data structure that can be used to address the raw bytes underlying a specific entry
 * in [FixedHareColumn] using a [TupleId] between 1 and [Long.MAX_VALUE].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
open class HareByteCursor(private val column: FixedHareColumn<*>, private val writeable: Boolean = false, start: TupleId = BYTE_CURSOR_BOF): AbstractInterruptibleChannel(), ByteCursor, Iterator<TupleId> {

    companion object {
        const val BYTE_CURSOR_BOF = 0L
        val OFFSET = 2 * Constants.PAGE_DATA_SIZE_BYTES /* Constant offset into HARE page file. Consists of page file header AND column header. */
    }

    /** A [StampedLock] for access to [HareCursor.tupleId]. */
    protected val addressLock = ReentrantReadWriteLock()

    /** Every [HareByteCursor] needs access to the [FixedHareColumn.Header]. */
    protected val header = this.column.Header()

    /** The bit shift required for [sizePerEntry]; i.e. the n in 2^n. */
    protected val shiftPerEntry = Integer.numberOfTrailingZeros(this.column.sizePerEntry)

    protected var tupleId: Long = start

    protected var start: Long = BYTE_CURSOR_BOF

    protected var end: Long = BYTE_CURSOR_BOF

    protected var startPageId: PageId = BYTE_CURSOR_BOF

    protected var endPageId = BYTE_CURSOR_BOF

    /** The maximum [TupleId] supported by this [ByteCursor]. */
    override val maximum: TupleId
        get() = this.header.count



    override fun read(dst: ByteBuffer): Int {
        if (!this.isOpen) { throw ClosedChannelException() }
        return this.addressLock.read {
            if (this.tupleId == BYTE_CURSOR_BOF) { throw IndexOutOfBoundsException("Cannot read without moving cursor position beyond BOF.")}
            val read = dst.remaining()

            /** Read first portion of the data (until end of first page). */
            val relativePosition = (this.start and Constants.PAGE_MOD_MASK_LONG).toInt()
            val limit = (Constants.PAGE_DATA_SIZE_BYTES-relativePosition)
            var page = this.column.bufferPool.get(this.startPageId, Priority.DEFAULT)

            if (read > limit) {
                dst.limit(dst.position() + limit)

                page.getBytes(relativePosition, dst)
                page.release()

                /** Read remaining data to remaining pages. */
                dst.limit(dst.capacity())
                for (pageId in (this.startPageId+1)..this.endPageId) {
                    page = this.column.bufferPool.get(pageId, Priority.DEFAULT)
                    page.getBytes(0, dst)
                    page.release()
                }
            } else {
                page.getBytes(relativePosition, dst)
                page.release()
            }

            read
        }
    }

    override fun write(src: ByteBuffer): Int {
        if (!this.writeable) { throw NonWritableChannelException() }
        if (!this.isOpen) { throw ClosedChannelException() }
        return this.addressLock.read {
            if (this.tupleId == BYTE_CURSOR_BOF) { throw IndexOutOfBoundsException("Cannot read without moving cursor position beyond BOF.")}

            val written = src.remaining()

            /** Write first portion of the data (unti end of first page). */
            val relativePosition = (this.start and Constants.PAGE_MOD_MASK_LONG).toInt()
            val limit = (Constants.PAGE_DATA_SIZE_BYTES-relativePosition)
            var page = this.column.bufferPool.get(this.startPageId, Priority.DEFAULT)

            if (written > limit) {
                src.limit(src.position() + limit)

                page.putBytes(relativePosition, src)
                page.release()

                src.limit(src.capacity())
                for (pageId in (this.startPageId+1)..this.endPageId) {
                    page = this.column.bufferPool.get(pageId, Priority.DEFAULT)
                    page.putBytes(0, src)
                    page.release()
                }
            } else {
                page.putBytes(relativePosition, src)
                page.release()
            }

            /* Reset src and return remaining bytes. */
            written
        }
    }

    override fun append() = this.addressLock.read {
        if (!this.writeable) { throw NonWritableChannelException() }
        if (!this.isOpen) { throw ClosedChannelException() }
        this.header.count += 1
        this.tupleId(this.maximum)
        if (this.end > this.column.bufferPool.totalPages) {
            for (i in kotlin.math.max(this.startPageId, this.column.bufferPool.totalPages)..this.endPageId) {
                this.column.bufferPool.append(Priority.DEFAULT).release()
            }
        }

    }
    override fun tupleId(): TupleId = this.addressLock.read {
        this.tupleId
    }
    /**
     *
     */
    override fun tupleId(new: TupleId) = this.addressLock.write {
        if (new < BYTE_CURSOR_BOF || new > this@HareByteCursor.maximum) {
            throw IndexOutOfBoundsException("Given tuple ID $new is out of bounds for this ByteCursor (max = ${this@HareByteCursor.maximum}).")
        }
        this.tupleId = new
        if (this.tupleId == BYTE_CURSOR_BOF) {
            this.end = BYTE_CURSOR_BOF
            this.start = BYTE_CURSOR_BOF
            this.startPageId = BYTE_CURSOR_BOF
            this.endPageId = BYTE_CURSOR_BOF
        } else {
            this.update()
        }
    }

    override fun next(): TupleId = this.addressLock.write {
        if ((++this.tupleId) > this@HareByteCursor.maximum) {
            throw IndexOutOfBoundsException("Given tuple ID ${--this.tupleId} is out of bounds for this ByteCursor (max = ${this@HareByteCursor.maximum}).")
        }
        this.update()
        this.tupleId
    }

    override fun previous(): TupleId = this.addressLock.write {
        if ((--this.tupleId) < BYTE_CURSOR_BOF) {
            throw IndexOutOfBoundsException("Given tuple ID ${++this.tupleId} is out of bounds for this ByteCursor (max = ${this@HareByteCursor.maximum}).")
        }
        if (this.tupleId == BYTE_CURSOR_BOF) {
            this.end = BYTE_CURSOR_BOF
            this.start = BYTE_CURSOR_BOF
            this.startPageId = BYTE_CURSOR_BOF
            this.endPageId = BYTE_CURSOR_BOF
        } else {
            this.update()
        }
        this.tupleId
    }

    override fun hasNext(): Boolean = this.addressLock.read {
        this.tupleId + 1  < this.maximum
    }

    override fun implCloseChannel() {}

    /**
     *
     */
    private fun update() {
        this.end = OFFSET + (this.tupleId shl this.shiftPerEntry) - 1
        this.start = this.end - this.column.sizePerEntry + 1
        this.startPageId = (this.start shr Constants.PAGE_BIT_SHIFT)
        this.endPageId = (this.end shr Constants.PAGE_BIT_SHIFT)
    }
}