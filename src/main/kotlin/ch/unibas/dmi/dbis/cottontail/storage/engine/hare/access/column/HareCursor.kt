package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.*
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import java.nio.ByteBuffer


/**
 * A cursor like data structure for access to the raw entries in a [HareColumn].
 *
 * *Important:* [HareCursor]s are NOT thread safe and their usage from multiple threads is not recommended!
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class HareCursor<T : Value>(private val bytes: ByteCursor, private val serializer: Serializer<T>) : ReadableCursor<T>, WritableCursor<T> {

    /** */
    private var entryHeader = ByteBuffer.allocate(HareColumn.ENTRY_HEADER_SIZE)

    override var position: TupleId
        get() = this.bytes.tupleId()
        set(v) {
            this.bytes.tupleId(v)
            this.bytes.read(this.entryHeader.reset()) /* Read header. */
        }

    override val maximum
        get() = this.bytes.maximum

    override fun next(): Boolean = this.bytes.next()

    override fun previous() = this.bytes.previous()

    /**
     * Returns a boolean indicating whether the entry the the current [HareCursor] position is null.
     *
     * @return true if the entry at the current position of the [HareCursor] is null and false otherwise.
     */
    override fun isNull(): Boolean = (this.entryHeader.getLong(0) and HareColumn.MASK_NULL) > 0L

    /**
     * Returns a boolean indicating whether the entry the the current [HareCursor] position has been deleted.
     *
     * @return true if the entry at the current position of the [HareCursor] has been deleted and false otherwise.
     */
    override fun isDeleted(): Boolean = (this.entryHeader.getLong(0) and HareColumn.MASK_DELETED) > 0L


    override fun get(): T? {
        if (this.isDeleted()) throw EntryDeletedException(this.position)
        if (this.isNull()) return null
        return this.serializer.deserialize(this.bytes)
    }

    override fun update(value: T?) {
        if (this.isDeleted()) throw EntryDeletedException(this.position)
        if (value == null) {
            this.entryHeader.putLong(this.entryHeader.getLong(0) or HareColumn.MASK_NULL)
            this.bytes.write(this.entryHeader)
        } else {
            this.serializer.serialize(this.bytes, value)
        }
    }

    override fun compareAndUpdate(expected: T?, newValue: T?): Boolean {
        if (this.isDeleted()) throw EntryDeletedException(this.position)

        return when {
            this.isNull() && expected == null -> {
                this.entryHeader.putLong(this.entryHeader.getLong(0) or HareColumn.MASK_NULL)
                this.bytes.write(this.entryHeader)
                true
            }
            this.isNull() && expected != null -> false
            else -> {
                val old = this.serializer.deserialize(this.bytes)
                if (old == expected) {
                    if (newValue == null) {
                        this.entryHeader.putLong(this.entryHeader.getLong(0) or HareColumn.MASK_NULL)
                        this.bytes.write(this.entryHeader)
                    } else {
                        this.serializer.serialize(this.bytes, newValue)
                    }
                    true
                } else {
                    false
                }
            }
        }
    }

    override fun append(value: T?) {
        this.bytes.append()
        this.update(value)
    }

    override fun delete(): T? {
        if (this.isDeleted()) throw EntryDeletedException(this.position)
        this.entryHeader.putLong(this.entryHeader.getLong(0) or HareColumn.MASK_DELETED)
        //TODO: Proper delete where old value is fully erased.
        this.bytes.write(this.entryHeader)
        return this.serializer.deserialize(this.bytes)
    }

    override fun close() = this.bytes.close()
}