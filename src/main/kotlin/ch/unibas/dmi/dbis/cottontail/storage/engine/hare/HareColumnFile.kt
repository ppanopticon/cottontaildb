package ch.unibas.dmi.dbis.cottontail.storage.engine.hare

import ch.unibas.dmi.dbis.cottontail.database.column.*
import ch.unibas.dmi.dbis.cottontail.model.exceptions.StoreException
import ch.unibas.dmi.dbis.cottontail.storage.store.MappedFileChannelStore

import java.nio.channels.FileChannel
import java.nio.file.Path


/**
 *
 *
 * [HARE]
 *
 */
class HareColumnFile(val file: Path, val readOnly: Boolean = false) {


    val store = MappedFileChannelStore(this.file, this.readOnly, true)


    /** */
    val header = this.store.map(0, 38, FileChannel.MapMode.READ_WRITE)

    /** */
    init {
        this.header.load() /* Force load data into memory. */

        /* Check hard-coded header. */
        assert(this.header.getChar(0) == 'H')
        assert(this.header.getChar(2) == 'A')
        assert(this.header.getChar(4) == 'R')
        assert(this.header.getChar(6) == 'E')

        /* Check the value of the columnSize and stripeSize property. */
        assert(this.header.getInt(18) > 0)
        assert(this.header.getInt(22) > this.header.getInt(18))
    }

    /** Version of this [HareColumnFile]. */
    val version: Short
        get() = this.header.getShort(8)

    /** The flags set in this [HareColumnFile]. */
    val type: ColumnType<*>
        get() = when(this.header.getShort(10).toInt())  {
            0 -> BooleanColumnType()
            1 -> ByteColumnType()
            2 -> ShortColumnType()
            3 -> IntColumnType()
            4 -> LongColumnType()
            5 -> FloatColumnType()
            6 -> DoubleColumnType()
            7 -> StringColumnType()
            8 -> BooleanVectorColumnType()
            9 -> IntVectorColumnType()
            10 -> LongVectorColumnType()
            11 -> FloatVectorColumnType()
            12 -> DoubleVectorColumnType()
            else -> throw StoreException("Wrong format!") /** TODO: Handle properly. */
        }

    /** The flags set in this [HareColumnFile]. */
    val flags: Int
        get() = this.header.getInt(12)

    /** Size of an individual column in bytes. */
    val columnSize: Int
        get() = this.header.getInt(16)

    /** Size of a stripe in bytes. */
    val stripeSize: Int
        get() = this.header.getInt(20)

    /** Size of a stripe in bytes. */
    var created: Long
        get() = this.header.getLong(24)
        private set(modified: Long) {
            this.header.putLong(24, modified)
        }

    /** Size of a stripe in bytes. */
    var modified: Long
        get() = this.header.getLong(32)
        private set(modified: Long) {
            this.header.putLong(32, modified)
        }

    /** The flags set in this [HareColumnFile]. */
    val nullable: Boolean
        get() = (this.flags and 1) == 1
}