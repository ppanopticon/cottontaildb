package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column.FixedHareColumn.Companion.MASK_NULL
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.EntryDeletedException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.TupleId
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.Priority
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonWritableChannelException
import kotlin.concurrent.read


/**
 * A cursor like data structure for access to the raw entries in a [FixedHareColumn].
 *
 * *Important:* [BufferedHareCursor]s are NOT thread safe and their usage from multiple threads is not recommended!
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class BufferedHareCursor<T : Value>(column: FixedHareColumn<T>, writeable: Boolean = false, start: TupleId = BYTE_CURSOR_BOF, private val serializer: Serializer<T>) : AbstractCursor<T>(column, writeable, start) {



    /**
     * Returns a boolean indicating whether the entry the the current [BufferedHareCursor] position is null.
     *
     * @return true if the entry at the current position of the [BufferedHareCursor] is null and false otherwise.
     */
    override fun isNull(): Boolean {
        if (!this.isOpen) { throw ClosedChannelException() }
        this.addressLock.read {
            if (this.tupleId == BYTE_CURSOR_BOF) { throw IndexOutOfBoundsException("Cannot read without moving cursor position beyond BOF.")}
            val page = this.column.bufferPool.get(this.pageId, Priority.DEFAULT)
            return (page.getLong(this.relativeOffset) and FixedHareColumn.MASK_NULL) > 0L
        }
    }

    /**
     * Returns a boolean indicating whether the entry the the current [BufferedHareCursor] position has been deleted.
     *
     * @return true if the entry at the current position of the [BufferedHareCursor] has been deleted and false otherwise.
     */
    override fun isDeleted(): Boolean {
        if (!this.isOpen) { throw ClosedChannelException() }
        this.addressLock.read {
            if (this.tupleId == BYTE_CURSOR_BOF) { throw IndexOutOfBoundsException("Cannot read without moving cursor position beyond BOF.")}
            val page = this.column.bufferPool.get(this.pageId, Priority.DEFAULT)
            return (page.getLong(this.relativeOffset) and FixedHareColumn.MASK_DELETED) > 0L
        }
    }


    override fun get(): T? {
        if (!this.isOpen) { throw ClosedChannelException() }
        this.addressLock.read {
            if (this.tupleId == BYTE_CURSOR_BOF) { throw IndexOutOfBoundsException("Cannot read without moving cursor position beyond BOF.")}
            val page = this.column.bufferPool.get(this.pageId, Priority.DEFAULT)
            if ((page.getLong(this.relativeOffset) and MASK_NULL) > 0L) return null
            if ((page.getLong(this.relativeOffset) and FixedHareColumn.MASK_DELETED) > 0L) throw EntryDeletedException(this.tupleId)
            val ret = this.serializer.deserialize(page, this.relativeOffset + FixedHareColumn.ENTRY_HEADER_SIZE)
            page.release()
            return ret
        }
    }

    override fun update(value: T?) {
        if (!this.writeable) { throw NonWritableChannelException() }
        if (!this.isOpen) { throw ClosedChannelException() }
        return this.addressLock.read {
            if (this.tupleId == BYTE_CURSOR_BOF) { throw IndexOutOfBoundsException("Cannot read without moving cursor position beyond BOF.")}
            val page = this.column.bufferPool.get(this.pageId, Priority.DEFAULT)
            if ((page.getLong(this.relativeOffset) and FixedHareColumn.MASK_DELETED) > 0L) throw EntryDeletedException(this.tupleId)
            if (value != null) {
                page.putLong(this.relativeOffset , (page.getLong(this.relativeOffset) and MASK_NULL.inv()))
                this.serializer.serialize(page, this.relativeOffset + FixedHareColumn.ENTRY_HEADER_SIZE, value)
            } else {
                page.putLong(this.relativeOffset , (page.getLong(this.relativeOffset) or MASK_NULL))
                for (i in (this.relativeOffset + FixedHareColumn.ENTRY_HEADER_SIZE)..this.entrySize) {
                    page.putByte(i, 0)
                }
            }
            page.release()
        }
    }

    override fun compareAndUpdate(expected: T?, newValue: T?): Boolean {
        TODO()
    }

    override fun append(value: T?) {

        if (!this.writeable) { throw NonWritableChannelException() }
        if (!this.isOpen) { throw ClosedChannelException() }

        this.addressLock.read {
            this.tupleId = this.header.count
            this.pageId = (this.tupleId / this.fillFactor) + 1
            this.relativeOffset = this.entrySize * ((this.tupleId % this.fillFactor).toInt())
            this.header.count++

            if (this.pageId >= this.column.bufferPool.totalPages) {
                /* Case 1: Data goes on new pages. */
                val page = this.column.bufferPool.detach()
                if (value != null) {
                    page.putLong(0, 1L)
                    this.serializer.serialize(page, FixedHareColumn.ENTRY_HEADER_SIZE, value)
                } else {
                    page.putLong(0, 1L and MASK_NULL)
                    for (i in (FixedHareColumn.ENTRY_HEADER_SIZE)..this.entrySize) {
                        page.putByte(i, 0)
                    }
                }
                this.column.bufferPool.append(page)
                page.release()
            } else {
                /* Case 2: Data goes on an existing page.*/
                val page = this.column.bufferPool.get(this.pageId, Priority.DEFAULT)
                if (value != null) {
                    page.putLong(this.relativeOffset , 1L)
                    this.serializer.serialize(page, this.relativeOffset + FixedHareColumn.ENTRY_HEADER_SIZE, value)
                } else {
                    page.putLong(this.relativeOffset , 1L and MASK_NULL)
                    for (i in (this.relativeOffset + FixedHareColumn.ENTRY_HEADER_SIZE)..this.entrySize) {
                        page.putByte(i, 0)
                    }
                }
                page.release()
            }
        }
    }

    override fun delete(): T? {
       TODO()
    }

    override fun implCloseChannel() {
    }
}