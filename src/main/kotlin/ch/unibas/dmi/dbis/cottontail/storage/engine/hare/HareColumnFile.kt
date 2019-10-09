package ch.unibas.dmi.dbis.cottontail.storage.engine.hare

import ch.unibas.dmi.dbis.cottontail.database.column.*
import ch.unibas.dmi.dbis.cottontail.model.exceptions.StoreException
import ch.unibas.dmi.dbis.cottontail.storage.store.MappedFileChannelStore
import java.nio.ByteBuffer

import java.nio.channels.FileChannel
import java.nio.file.Path

/**
 *
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class HareColumnFile<T: Any>(val file: Path, val readOnly: Boolean = false) {

    companion object {
        const val DEFINITION_VERSION = 1.toShort()

        /** Size of a [HareColumnFile] header in bytes. */
        const val HEADER_SIZE_BYTES = 56

        /** Size of a [HareColumnFile.Stripe] header in bytes. */
        const val STRIPE_HEADER_SIZE_BYTES = 24
    }

    /** Internal reference to [MappedFileChannelStore]. */
    private val store = MappedFileChannelStore(this.file, this.readOnly, true)

    /** The header of the file is directly mapped to memory, which allows for direct access. */
    private val header = this.store.map(0, 38, if (this.readOnly) { FileChannel.MapMode.READ_ONLY } else { FileChannel.MapMode.READ_WRITE })

    /** */
    init {
        this.header.load() /* Force load data into memory. */

        /* Check hard-coded header. */
        assert(this.header.get(0).toChar() == 'H')
        assert(this.header.get(1).toChar() == 'A')
        assert(this.header.get(2).toChar() == 'R')
        assert(this.header.get(3).toChar() == 'E')

        /* Check version of HARE file. */
        assert(this.header.getShort(4) == DEFINITION_VERSION)

        /* Check the value of the columnSize and stripeSize property. */
        assert(this.header.getInt(8) > 0)
        assert(this.header.getInt(12) > this.header.getInt(8))
    }


    /** The raw flags set in this [HareColumnFile]. */
    val flags: Long
        get() = this.header.getLong(16)

    /** The raw flags set in this [HareColumnFile]. */
    val firstStripeOffset: Int
        get() = this.header.getInt(52)


    /** Returns a [HareColumnMetadata] object for this [HareColumnFile]. */
    val metadata: HareColumnMetadata = object : HareColumnMetadata {

        /** Version of this [HareColumnFile]. */
        override val version: Short
            get() = this@HareColumnFile.header.getShort(4)

        /** The [ColumnType] held by this [HareColumnFile]. */
        override val type: ColumnType<T>
            get() = when(this@HareColumnFile.header.getShort(6).toInt())  {
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
            } as ColumnType<T>

        /** Whether or not entries in this [HareColumnFile] are nullable. */
        override val nullable: Boolean
            get() = (this@HareColumnFile.flags and 1L) == 1L

        /** Size of an individual column in bytes. */
        override val columnSize: Int
            get() = this@HareColumnFile.header.getInt(8)

        /** Size of a stripe in bytes. */
        override val stripeSize: Int
            get() = this@HareColumnFile.header.getInt(12)

        /** Number of stripes in this [HareColumnFile]. */
        override val stripes: Int
            get() = (Math.floorDiv((this.stripeSize - STRIPE_HEADER_SIZE_BYTES).toLong(), this.rows * this.columnSize) + 1L).toInt()

        /** Number of (un-deleted) rows in a [HareColumnFile]. */
        override val rows: Long
            get() = this@HareColumnFile.header.getLong(24)

        /** Number of deleted rows in a [HareColumnFile]. */
        override val deleted: Long
            get() = this@HareColumnFile.header.getLong(32)
    }

    /**
     *
     */
    operator fun get(rowId: Long) {

    }


    /**
     * Represents an individual stripe within a [HareColumnFile].
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class Stripe(stripeId: Int) {

        /** The [ByteBuffer] that holds this [Stripe]'s data. Whenever a [Stripe] is instantiated, the entire [Stripe] is loaded into memory. */
        val buffer: ByteBuffer = ByteBuffer.allocateDirect(this@HareColumnFile.metadata.stripeSize)

        /** */
        init {
            this@HareColumnFile.store.getData(this@HareColumnFile.firstStripeOffset.toLong() + this@HareColumnFile.metadata.stripeSize.toLong() * stripeId.toLong(), this.buffer)
        }

    }
}