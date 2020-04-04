package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.TupleIdOutOfBoundException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.ReadableCursor
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.ReadableCursor.Companion.BYTE_CURSOR_BOF
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.TupleId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.WritableCursor
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId
import java.lang.Math.floorDiv
import java.nio.channels.spi.AbstractInterruptibleChannel
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock
import kotlin.concurrent.read
import kotlin.concurrent.write

abstract class AbstractCursor<T: Value>(protected val column: FixedHareColumn<T>, protected val writeable: Boolean, start: TupleId): AbstractInterruptibleChannel(), ReadableCursor<T>, WritableCursor<T> {
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
     * Sets this [AbstractCursor]'s [TupleId] to the given [TupleId].
     *
     * @param new The new, desired [TupleId] position.
     * @return New value for [TupleId]
     * @throws [TupleIdOutOfBoundException] If [TupleId] is out of bounds (i.e. > [ReadableCursor.maximum] or < [BYTE_CURSOR_BOF]).
     */
    override fun tupleId(): TupleId = this.addressLock.read {
        this.tupleId
    }

    /**
     * Sets this [AbstractCursor]'s [TupleId] to the given [TupleId].
     *
     * @param new The new, desired [TupleId] position.
     * @return New value for [TupleId]
     * @throws [TupleIdOutOfBoundException] If [TupleId] is out of bounds (i.e. > [ReadableCursor.maximum] or < [BYTE_CURSOR_BOF]).
     */
    override fun tupleId(new: TupleId): TupleId = this.addressLock.write {
        if (new < BYTE_CURSOR_BOF || new > this.maximum) {
            throw TupleIdOutOfBoundException("Given tuple ID $new is out of bounds for this ByteCursor (max = ${this.maximum}).")
        }
        this.tupleId = new
        if (this.tupleId == BYTE_CURSOR_BOF) {
            this.pageId = BYTE_CURSOR_BOF
        } else {
            this.pageId = ((this.tupleId / this.fillFactor) + 1)
            this.relativeOffset = this.entrySize * ((this.tupleId % this.fillFactor).toInt())
        }
        return new
    }

    /**
     * Moves this [AbstractCursor]'s [TupleId] to the next position.
     *
     * @return New [TupleId]
     * @throws [TupleIdOutOfBoundException] If next [TupleId] is out of bounds (i.e. > [ReadableCursor.maximum]).
     */
    override fun next(): TupleId = this.tupleId(this.tupleId + 1)

    /**
     * Moves this [AbstractCursor]'s [TupleId] to the previous position.
     *
     * @return New [TupleId]
     * @throws [TupleIdOutOfBoundException] If previous [TupleId] is out of bounds (i.e. < [BYTE_CURSOR_BOF]).
     */
    override fun previous(): TupleId = this.tupleId(this.tupleId - 1)

    /**
     * Checks if this [AbstractCursor] has a valid [TupleId] beyond the current [TupleId]. If this method
     * returns true, then the next call to [AbstractCursor.next] is guaranteed to be safe.
     *
     * @return True if there exists a valid [TupleId] beyond the current one, false otherwise.
     */
    override fun hasNext(): Boolean = this.addressLock.read {
        this.tupleId < this.maximum
    }
}