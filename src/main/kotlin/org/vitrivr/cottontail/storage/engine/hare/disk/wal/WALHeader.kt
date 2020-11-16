package org.vitrivr.cottontail.storage.engine.hare.disk.wal

import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.basics.View
import org.vitrivr.cottontail.storage.engine.hare.disk.FileType
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * A view on the header section of a [WriteAheadLog] file.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class WALHeader : View {

    companion object {
        /** Size of this HARE WAL file header. */
        const val SIZE = 128

        /** Version of the HARE WAL file. */
        const val WAL_VERSION = 1

        /** Identifier of every HARE file. */
        val FILE_HEADER_IDENTIFIER = charArrayOf('H', 'A', 'R', 'E')

        /** Offsets. */

        /** The offset into a [WALHeader] to get its type. */
        private const val HEADER_OFFSET_TYPE = 8

        /** The offset into a [WALHeader] to get its version. */
        private const val HEADER_OFFSET_VERSION = 12

        /** The offset into a [WALHeader] to get its flags. */
        private const val HEADER_OFFSET_FLAGS = 16

        /** The offset into a [WALHeader] to get the number of allocated pages. */
        private const val HEADER_OFFSET_LOG = 24

        /** The offset into a [WALHeader] to get the number of pre-allocated pages. */
        private const val HEADER_OFFSET_LOG_TRANSFERRED = 32

        /** The offset into a [WALHeader] to get the number of pre-allocated pages. */
        private const val HEADER_OFFSET_CHECKSUM = 40

        /** Masks. */

        /** Mask for consistency flag in in this [WALHeader]. */
        const val HEADER_MASK_CONSISTENCY_OK = 1L shl 0
    }

    /** The [ByteBuffer] backing this [WALHeader]. */
    override val buffer: ByteBuffer = ByteBuffer.allocate(SIZE)

    /** Type of the file containing this [WALHeader] (must be [FileType.WAL]). */
    val type: FileType
        get() = FileType.values()[this.buffer.getInt(HEADER_OFFSET_TYPE)]

    /** Version of the file containing this [WALHeader]. */
    val version: Int
        get() = this.buffer.getInt(HEADER_OFFSET_VERSION)

    /** Total number of entries contained in this [WriteAheadLog] file. */
    val flags: Long
        get() = this.buffer.getLong(HEADER_OFFSET_FLAGS)

    /** Total number of entries contained in this [WriteAheadLog] file. */
    var entries: Long
        get() = this.buffer.getLong(HEADER_OFFSET_LOG)
        set(v) {
            this.buffer.putLong(HEADER_OFFSET_LOG, v)
        }

    /** Total number of entries contained in this [WriteAheadLog] file. */
    var transferred: Long
        get() = this.buffer.getLong(HEADER_OFFSET_LOG_TRANSFERRED)
        set(v) {
            this.buffer.putLong(HEADER_OFFSET_LOG_TRANSFERRED, v)
        }

    /** Total number of entries contained in this [WriteAheadLog] file. */
    var checksum: Long
        get() = this.buffer.getLong(HEADER_OFFSET_CHECKSUM)
        set(v) {
            this.buffer.putLong(HEADER_OFFSET_CHECKSUM, v)
        }


    /**
     * Initializes a new [WALHeader], which will overwrite its contet.
     *
     * @return This [WALHeader]
     */
    fun init(): WALHeader {
        this.buffer.putChar(FILE_HEADER_IDENTIFIER[0])             /* 0: Identifier H. */
        this.buffer.putChar(FILE_HEADER_IDENTIFIER[1])             /* 2: Identifier A. */
        this.buffer.putChar(FILE_HEADER_IDENTIFIER[2])             /* 4: Identifier R. */
        this.buffer.putChar(FILE_HEADER_IDENTIFIER[3])             /* 6: Identifier E. */
        this.buffer.putInt(FileType.WAL.ordinal)                   /* 8: Type of HARE file. */
        this.buffer.putInt(WAL_VERSION)                            /* 12: Version of the HARE format. */
        this.buffer.putLong(HEADER_MASK_CONSISTENCY_OK)            /* 20: Flags used by the HARE page file. */
        this.buffer.putLong(0L)                              /* 28: Number of pages in WAL file. */
        this.buffer.putLong(0L)                              /* 36: Number of transferred pages in WAL file. */
        this.buffer.putLong(0L)                              /* 44: CRC32 checksum for WAL file. */
        return this                                                /* 52-127: For future use. */
    }

    /**
     * Reads the content of this [WALHeader] from the given [FileChannel].
     *
     * @param channel The [FileChannel] to read from.
     * @param position The position in the [FileChannel] to write to.
     */
    override fun read(channel: FileChannel, position: Long): WALHeader {
        channel.read(this.buffer.rewind(), position)
        this.validate()
        return this
    }

    /**
     * Reads the content of this [WALHeader] from the given [FileChannel].
     *
     * @param channel The [FileChannel] to read from.
     */
    override fun read(channel: FileChannel): View {
        channel.read(this.buffer.rewind())
        this.validate()
        return this
    }

    /**
     * Writes the content of this [WALHeader] to the given [FileChannel].
     *
     * @param channel The [FileChannel] to write to.
     * @param position The position in the [FileChannel] to write to.
     */
    override fun write(channel: FileChannel, position: Long): WALHeader {
        channel.write(this.buffer.rewind(), position)
        return this
    }

    /**
     * Writes the content of this [WALHeader] to the given [FileChannel].
     *
     * @param channel The [FileChannel] to write to.
     */
    override fun write(channel: FileChannel): View {
        channel.write(this.buffer.rewind())
        return this
    }

    /**
     * Validates this [WALHeader]
     */
    private fun validate() {
        /* Prepare buffer to read. */
        this.buffer.rewind()

        /* Make necessary check on reading. */
        if (this.buffer.char != FILE_HEADER_IDENTIFIER[0]) {
            throw DataCorruptionException("HARE identifier missing in HARE WAL file.")
        }
        if (this.buffer.char != FILE_HEADER_IDENTIFIER[1]) {
            throw DataCorruptionException("HARE identifier missing in HARE WAL file.")
        }
        if (this.buffer.char != FILE_HEADER_IDENTIFIER[2]) {
            throw DataCorruptionException("HARE identifier missing in HARE WAL file.")
        }
        if (this.buffer.char != FILE_HEADER_IDENTIFIER[3]) {
            throw DataCorruptionException("HARE identifier missing in HARE WAL file.")
        }
        if (this.buffer.int != FileType.WAL.ordinal) {
            throw DataCorruptionException("HARE file type mismatch in HARE WAL file.")
        }
        if (this.buffer.int != WAL_VERSION) {
            throw DataCorruptionException("HARE file version mismatch in HARE file.")
        }
        this.buffer.long
        if (this.buffer.long < 0) {
            throw DataCorruptionException("Negative number of entries found in HARE WAL file.")
        }
        if (this.buffer.long < 0) {
            throw DataCorruptionException("Negative number of transferred entries found in HARE WAL file.")
        }

        /* ToDo: CRC32 check. */
    }
}