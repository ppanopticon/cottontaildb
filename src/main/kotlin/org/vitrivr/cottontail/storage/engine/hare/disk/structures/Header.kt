package org.vitrivr.cottontail.storage.engine.hare.disk.structures

import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.basics.View
import org.vitrivr.cottontail.storage.engine.hare.disk.FileType
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * A view on the header section of a [org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager].
 *
 * @see org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager
 *
 * @version 1.1.2
 * @author Ralph Gasser
 */
class Header(val direct: Boolean = false) : View {
    companion object {
        /** Constants. */

        /** Version of the HARE file. */
        const val FILE_HEADER_VERSION = 1

        /** Identifier of every HARE file. */
        private val FILE_HEADER_IDENTIFIER = charArrayOf('H', 'A', 'R', 'E')

        /** Sizes. */

        /** Size of the HARE page file header in bytes. */
        const val SIZE = 128

        /** Offsets. */

        /** The offset into a [Header] to get its type. */
        private const val HEADER_OFFSET_TYPE = 8

        /** The offset into a [Header] to get its version. */
        private const val HEADER_OFFSET_VERSION = 12

        /** The offset into a [Header] to get its size. */
        private const val HEADER_OFFSET_SIZE = 16

        /** The offset into a [Header] to get its flags. */
        private const val HEADER_OFFSET_FLAGS = 20

        /** The offset into a [Header] to get the number of allocated pages. */
        private const val HEADER_OFFSET_ALLOCATED = 28

        /** The offset into a [Header] to get the number of dangling pages. */
        private const val HEADER_OFFSET_DANGLING = 36

        /** The offset into a [Header] to get the checksum for the file. */
        private const val HEADER_OFFSET_CHECKSUM = 44

        /** Masks. */

        /** Mask for consistency flag in in this [Header]. */
        const val HEADER_MASK_CONSISTENCY_OK = 1L shl 0
    }

    /** The [ByteBuffer] that backs this [Header]. */
    override val buffer: ByteBuffer = if (this.direct) {
        ByteBuffer.allocate(SIZE)
    } else {
        ByteBuffer.allocateDirect(SIZE)
    }

    /** Type of the file containing this [Header] (must be [FileType.DEFAULT]). */
    val type: FileType
        get() = FileType.values()[this.buffer.getInt(HEADER_OFFSET_TYPE)]

    /** Version of the file containing this [Header]. */
    val version: Int
        get() = this.buffer.getInt(HEADER_OFFSET_VERSION)

    /** The bit shift used to determine the [Page] size of the HARE file this [Header] belongs to. */
    val pageShift: Int
        get() = this.buffer.getInt(HEADER_OFFSET_SIZE)

    /** The [Page] size of the page file. */
    val pageSize: Int
        get() = 1 shl this.pageShift

    /** Flags set in this [Header]. */
    var flags: Long
        get() = this.buffer.getLong(HEADER_OFFSET_FLAGS)
        set(v) {
            this.buffer.putLong(HEADER_OFFSET_FLAGS, v)
        }

    /** Sets file sanity byte according to consistency status. */
    var isConsistent: Boolean
        get() = (this.flags and HEADER_MASK_CONSISTENCY_OK) == HEADER_MASK_CONSISTENCY_OK
        set(v) {
            if (v) {
                this.flags = this.flags or HEADER_MASK_CONSISTENCY_OK
            } else {
                this.flags = this.flags and HEADER_MASK_CONSISTENCY_OK.inv()
            }
        }

    /** Total number of [Page]s managed by the HARE file this [Header] belongs to. */
    var allocatedPages: Long
        get() = this.buffer.getLong(HEADER_OFFSET_ALLOCATED)
        set(v) {
            this.buffer.putLong(HEADER_OFFSET_ALLOCATED, v)
        }

    /** Number of dangling [Page]s in the HARE file this [Header] belongs to. */
    var danglingPages: Long
        get() = this.buffer.getLong(HEADER_OFFSET_DANGLING)
        set(v) {
            this.buffer.putLong(HEADER_OFFSET_DANGLING, v)
        }

    /** CRC32C checksum for the HARE file this [Header] belongs to. */
    var checksum: Long
        get() = this.buffer.getLong(HEADER_OFFSET_CHECKSUM)
        set(v) {
            this.buffer.putLong(HEADER_OFFSET_CHECKSUM, v)
        }

    /**
     * Initializes this [ByteBuffer] as new [Header].
     *
     * @param pageShift The [pageShift] constant, which is configurable.
     * @return This [Header]
     */
    fun init(pageShift: Int): Header {
        this.buffer.putChar(FILE_HEADER_IDENTIFIER[0])             /* 0: Identifier H. */
        this.buffer.putChar(FILE_HEADER_IDENTIFIER[1])             /* 2: Identifier A. */
        this.buffer.putChar(FILE_HEADER_IDENTIFIER[2])             /* 4: Identifier R. */
        this.buffer.putChar(FILE_HEADER_IDENTIFIER[3])             /* 6: Identifier E. */
        this.buffer.putInt(FileType.DEFAULT.ordinal)               /* 8: Type of HARE file. */
        this.buffer.putInt(FILE_HEADER_VERSION)                    /* 12: Version of the HARE format. */
        this.buffer.putInt(pageShift)                              /* 16: Size of a HARE page; stored as bit shift. */
        this.buffer.putLong(HEADER_MASK_CONSISTENCY_OK)            /* 20: Flags used by the HARE page file. */
        this.buffer.putLong(0L)                              /* 28: Allocated page counter; number of allocated pages. */
        this.buffer.putLong(0L)                              /* 36: CRC32 checksum for HARE file. */
        return this                                                /* 44-127: For future use. */
    }

    /**
     * Reads the content of this [Header] from the given [FileChannel].
     *
     * @param channel The [FileChannel] to read from.
     * @param position The position in the [FileChannel] to write to.
     * @return This [Header]
     */
    override fun read(channel: FileChannel, position: Long): Header {
        channel.read(this.buffer.rewind(), position)
        this.validate()
        return this
    }

    /**
     * Reads the content of this [Header] from the given [FileChannel].
     *
     * @param channel The [FileChannel] to read from.
     * @return This [Header]
     */
    override fun read(channel: FileChannel): Header {
        channel.read(this.buffer.rewind())
        this.validate()
        return this
    }

    /**
     * Writes the content of this [Header] to the given [FileChannel].
     *
     * @param channel The [FileChannel] to write to.
     * @param position The position in the [FileChannel] to write to.
     * @return This [Header]
     */
    override fun write(channel: FileChannel, position: Long): Header {
        channel.write(this.buffer.rewind(), position)
        return this
    }

    /**
     * Writes the content of this [Header] to the given [FileChannel].
     *
     * @param channel The [FileChannel] to write to.
     * @return This [Header]
     */
    override fun write(channel: FileChannel): View {
        channel.write(this.buffer.rewind())
        return this
    }

    /**
     * Validates this [Header].
     */
    private fun validate() {
        /* Prepare buffer to read. */
        this.buffer.rewind()

        /* Make necessary check on reading. */
        require(this.buffer.char == FILE_HEADER_IDENTIFIER[0]) { DataCorruptionException("HARE identifier missing in HARE page file.") }
        require(this.buffer.char == FILE_HEADER_IDENTIFIER[1]) { DataCorruptionException("HARE identifier missing in HARE page file.") }
        require(this.buffer.char == FILE_HEADER_IDENTIFIER[2]) { DataCorruptionException("HARE identifier missing in HARE page file.") }
        require(this.buffer.char == FILE_HEADER_IDENTIFIER[3]) { DataCorruptionException("HARE identifier missing in HARE page file.") }
        require(this.buffer.int == FileType.DEFAULT.ordinal)
        require(this.buffer.int == FILE_HEADER_VERSION) { DataCorruptionException("File version mismatch in HARE page file.") }
        require(this.buffer.int >= 10) { DataCorruptionException("Page shift mismatch in HARE page file.") }
        this.buffer.long
        require(this.buffer.long >= 0) { DataCorruptionException("Negative number of allocated pages found in HARE page file.") }
    }
}