package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.*
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import java.nio.ByteBuffer


/**
 * A cursor like data structure for access to the raw entries in a [FixedHareColumn].
 *
 * *Important:* [HareCursor]s are NOT thread safe and their usage from multiple threads is not recommended!
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class HareCursor<T : Value>(column: FixedHareColumn<T>, writeable: Boolean = false, start: TupleId = BYTE_CURSOR_BOF, private val serializer: Serializer<T>) : HareByteCursor(column, writeable, start), ReadableCursor<T>, WritableCursor<T> {

    /** */
    private var entryHeader = ByteBuffer.allocate(FixedHareColumn.ENTRY_HEADER_SIZE)

    /**
     * Returns a boolean indicating whether the entry the the current [HareCursor] position is null.
     *
     * @return true if the entry at the current position of the [HareCursor] is null and false otherwise.
     */
    override fun isNull(): Boolean = (this.entryHeader.getLong(0) and FixedHareColumn.MASK_NULL) > 0L

    /**
     * Returns a boolean indicating whether the entry the the current [HareCursor] position has been deleted.
     *
     * @return true if the entry at the current position of the [HareCursor] has been deleted and false otherwise.
     */
    override fun isDeleted(): Boolean = (this.entryHeader.getLong(0) and FixedHareColumn.MASK_DELETED) > 0L


    override fun get(): T? {
        if (this.isDeleted()) throw EntryDeletedException(this.tupleId)
        if (this.isNull()) return null
        return this.serializer.deserialize(this)
    }

    override fun update(value: T?) {
        if (this.isDeleted()) throw EntryDeletedException(this.tupleId)
        if (value == null) {
            this.entryHeader.putLong(this.entryHeader.getLong(0) or FixedHareColumn.MASK_NULL)
            this.write(this.entryHeader)
        } else {
            this.serializer.serialize(this, value)
        }
    }

    override fun compareAndUpdate(expected: T?, newValue: T?): Boolean {
        if (this.isDeleted()) throw EntryDeletedException(this.tupleId)

        return when {
            this.isNull() && expected == null -> {
                this.entryHeader.putLong(this.entryHeader.getLong(0) or FixedHareColumn.MASK_NULL)
                this.write(this.entryHeader)
                true
            }
            this.isNull() && expected != null -> false
            else -> {
                val old = this.serializer.deserialize(this)
                if (old == expected) {
                    if (newValue == null) {
                        this.entryHeader.putLong(this.entryHeader.getLong(0) or FixedHareColumn.MASK_NULL)
                        this.write(this.entryHeader)
                    } else {
                        this.serializer.serialize(this, newValue)
                    }
                    true
                } else {
                    false
                }
            }
        }
    }

    override fun append(value: T?) {
        this.append()
        this.update(value)
    }

    override fun delete(): T? {
        if (this.isDeleted()) throw EntryDeletedException(this.tupleId)
        this.entryHeader.putLong(this.entryHeader.getLong(0) or FixedHareColumn.MASK_DELETED)
        //TODO: Proper delete where old value is fully erased.
        this.write(this.entryHeader)
        return this.serializer.deserialize(this)
    }
}