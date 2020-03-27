package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.ReadableCursor
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.TupleId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.WritableCursor
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId
import java.lang.Math.floorDiv
import java.nio.channels.spi.AbstractInterruptibleChannel
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock
import kotlin.concurrent.read
import kotlin.concurrent.write

abstract class AbstractCursor<T: Value>(protected val column: FixedHareColumn<T>, protected val writeable: Boolean, start: TupleId): AbstractInterruptibleChannel(), ReadableCursor<T>, WritableCursor<T>, Iterator<TupleId> {
    companion object {
        const val BYTE_CURSOR_BOF = -1L
    }

    protected val pageShift = this.column.disk.pageShift

    protected val pageSize = 1 shl this.pageShift

    protected val entrySize = this.column.entrySize

    protected val fillFactor = floorDiv(this.pageSize, this.entrySize)

    /** A [StampedLock] for access to [BufferedHareCursor.tupleId]. */
    protected val addressLock = ReentrantReadWriteLock()

    /** Every [HareByteCursor] needs access to the [FixedHareColumn.Header]. */
    protected val header = this.column.Header()

    /** The bit shift required for [sizePerEntry]; i.e. the n in 2^n. */

    protected var pageId: PageId = BYTE_CURSOR_BOF

    protected var tupleId: Long = start

    protected var relativeOffset: Int = -1

    /** The maximum [TupleId] supported by this [AbstractCursor]. */
    override val maximum: TupleId
        get() = this.header.count - 1

    /**
     *
     */
    override fun tupleId(): TupleId = this.addressLock.read {
        this.tupleId
    }

    /**
     *
     */
    override fun tupleId(new: TupleId) = this.addressLock.write {
        if (new < BYTE_CURSOR_BOF || new > this.maximum) {
            throw IndexOutOfBoundsException("Given tuple ID $new is out of bounds for this ByteCursor (max = ${this.maximum}).")
        }
        this.tupleId = new
        if (this.tupleId == BYTE_CURSOR_BOF) {
            this.pageId = BYTE_CURSOR_BOF
        } else {
            this.pageId = ((this.tupleId / this.fillFactor) + 1)
            this.relativeOffset = this.entrySize * ((this.tupleId % this.fillFactor).toInt())
        }
    }

    override fun next(): TupleId = this.addressLock.write {
        if (this.tupleId >= this.maximum) {
            throw IndexOutOfBoundsException("Given tuple ID ${this.tupleId + 1} is out of bounds for this ByteCursor (max = ${this.maximum}).")
        }
        this.tupleId(this.tupleId + 1)
        this.tupleId
    }

    override fun previous(): TupleId = this.addressLock.write {
        if (this.tupleId <= BYTE_CURSOR_BOF) {
            throw IndexOutOfBoundsException("Given tuple ID ${this.tupleId-1} is out of bounds for this ByteCursor (max = ${this.maximum}).")
        }
        this.tupleId(this.tupleId - 1)
        this.tupleId
    }

    override fun hasNext(): Boolean = this.addressLock.read {
        this.tupleId < this.maximum
    }
}