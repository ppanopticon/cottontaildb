package org.vitrivr.cottontail.storage.engine.hare.disk

import org.vitrivr.cottontail.storage.basics.MemorySize
import org.vitrivr.cottontail.storage.basics.Units
import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.basics.Resource

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock
import java.util.zip.CRC32C

/**
 * The [DiskManager] facilitates reading and writing of [Page]s from/to the underlying HARE page file
 * usually residing on some form of persistent storage. Only one  [DiskManager] can be opened per HARE
 * page fil and it acquires an exclusive [FileLock] once created.
 *
 * @version 1.1
 * @author Ralph Gasser
 */
abstract class DiskManager(val path: Path, val lockTimeout: Long = 5000) : Resource {

    companion object {

        /** Size of the HARE page file header. */
        const val FILE_HEADER_SIZE = 64

        /** Minimum page shift, results in a minimum page size of 4069 bytes. */
        const val MIN_PAGE_SHIFT = 12

        /** Maximum page shift, results in a maximum page size of 4'194'304 bytes. */
        const val MAX_PAGE_SHIFT = 22

        /** Identifier of every HARE file. */
        val FILE_HEADER_IDENTIFIER = charArrayOf('H', 'A', 'R', 'E')

        /** Version of the HARE file. */
        const val FILE_HEADER_VERSION = 1.toByte()

        /** Flag for when the file's consistency can be considered OK. Exact semantic depends on [DiskManager] implementation.  */
        const val FILE_CONSISTENCY_OK = 0.toByte()

        /** Flag for when the file's consistency must be checked. Exact semantic depends on [DiskManager] implementation.  */
        const val FILE_SANITY_CHECK = 1.toByte()

        /**
         * Creates a new page file in the HARE format.
         *
         * @param path [Path] under which to create the page file.
         */
        fun create(path: Path, pageShift: Int = 18) {
            /* Prepare header data for page file in the HARE format. */
            val data: ByteBuffer = ByteBuffer.allocateDirect(FILE_HEADER_SIZE)
            data.putChar(FILE_HEADER_IDENTIFIER[0])             /* 0: Identifier H. */
            data.putChar(FILE_HEADER_IDENTIFIER[1])             /* 2: Identifier A. */
            data.putChar(FILE_HEADER_IDENTIFIER[2])             /* 4: Identifier R. */
            data.putChar(FILE_HEADER_IDENTIFIER[3])             /* 6: Identifier E. */
            data.putInt(FileType.DEFAULT.ordinal)               /* 8: Type of HARE file. */
            data.put(FILE_HEADER_VERSION)                       /* 12: Version of the HARE format. */
            data.putInt(pageShift)                              /* 13: Size of a HARE page. Indicated as bit shift. */
            data.put(FILE_CONSISTENCY_OK)                       /* 17: Sanity byte; exact semantic depends on implementation. */
            data.putLong(0L)                              /* 18: Page counter; number of pages. */
            data.putInt(0)                                /* 26: Page counter; number of freed pages. */
            data.putLong(0L)                              /* 30: CRC32 checksum for HARE file. */

            /** Write data to file and close. */
            val channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, StandardOpenOption.SYNC, StandardOpenOption.SPARSE)
            channel.write(data.rewind())
            channel.close()
        }
    }

    /** The [FileChannel] used to access the file managed by this [DiskManager]. */
    protected val fileChannel: FileChannel = FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.DSYNC, StandardOpenOption.SPARSE)

    /** Acquires an exclusive [FileLock] for file underlying this [FileChannel]. Makes sure, that no other process uses the same HARE file. */
    protected val fileLock = FileUtilities.acquireFileLock(this.fileChannel, this.lockTimeout)

    /** Accessor to the [Header] of the HARE file managed by this [DiskManager]. */
    protected val header = Header()

    /** A [ReentrantReadWriteLock] that mediates access to the closed state of this [DiskManager]. */
    protected val closeLock = StampedLock()

    /** The bit shift used to determine the [Page] size of the page file managed by this [DiskManager]. */
    val pageShift
        get() = this.header.pageShift

    /** The [Page] size of the page file managed by this [DiskManager]. */
    val pageSize
        get() = this.header.pageSize

    /** Number of [Page]s held by the HARE page file managed by this [DiskManager]. */
    val pages
        get() = this.header.pages

    /** Returns the size of the HARE page file managed by this [DiskManager]. */
    val size
        get() = MemorySize(this.fileChannel.size().toDouble(), Units.BYTE)

    /** Return true if this [FileChannel] and thus this [DiskManager] is still open. */
    override val isOpen
        get() = this.fileChannel.isOpen

    /**
     * Fetches the data identified by the given [PageId] into the given [Page] object thereby replacing the content of that [Page].
     *
     * @param id [PageId] to fetch data for.
     * @param page [DataPage] to fetch data into. Its content will be updated.
     */
    abstract fun read(id: PageId, page: DataPage)

    /**
     * Fetches the data starting from the given [PageId] into the given [Page] objects thereby replacing the content of those [Page].
     *
     * @param startId [PageId] to start fetching
     * @param pages [Page]s to fetch data into. Their content will be updated.
     */
    abstract fun read(startId: PageId, pages: Array<DataPage>)

    /**
     * Updates the [Page] identified by the given [PageId] in the HARE file managed by this [DiskManager].
     *
     * @param id [PageId] of the [Page] that should be updated
     * @param page [Page] the data the [Page] should be updated with.
     */
    abstract fun update(id: PageId, page: DataPage)

    /**
     * Allocates new [Page] in the HARE file managed by this [DiskManager].
     *
     * @param page [Page] to append. If empty, the allocated [Page] will be filled with zeros.
     */
    abstract fun allocate(page: DataPage? = null): PageId

    /**
     * Frees the [Page] identified by the given [PageId] making space for new entries
     *
     * @param id The [PageId] that should be freed.
     */
    abstract fun free(id: PageId)

    /**
     * Commits all changes made through this [DiskManager].
     */
    abstract fun commit()

    /**
     * Rolls back all changes made through this [DiskManager].
     */
    abstract fun rollback()

    /**
     * Deletes the HARE file backing this [DiskManager]. Calling this method also
     * closes the associated [FileChannel].
     */
    open fun delete() {
        this.close()
        Files.delete(this.path)
    }

    /**
     * Calculates the [CRC32C] checksum for the file managed by this [DiskManager].
     *
     * @return [CRC32C] object for this [DiskManager]
     */
    fun calculateChecksum(): Long {
        val page = ByteBuffer.allocateDirect(this.header.pageSize)
        val crc32 = CRC32C()
        for (i in 1..this.pages) {
            this.fileChannel.read(page, this.pageIdToPosition(i))
            crc32.update(page.flip())
        }
        return crc32.value
    }

    /**
     * Validation method. Compares the checksum in the file's [Header] to the actual checksum of the content.
     *
     * @return true If and only if checksum in header and of content are identical.
     */
    fun validate(): Boolean = this.header.checksum == this.calculateChecksum()

    /**
     * Converts the given [PageId] to an offset into the file managed by this [DirectDiskManager]. Calling this method
     * also makes necessary sanity checks regarding the file's channel status and pageId bounds.
     *
     * @param pageId The [PageId] to translate to a position.
     * @return The offset into the file.
     */
    protected fun pageIdToPosition(pageId: PageId): Long {
        require(pageId <= this.header.pages && pageId >= 0) { "The given page ID $pageId is out of bounds for this HARE page file (file: ${this.path}, pages: ${this.pages})." }
        return FILE_HEADER_SIZE + (pageId.toLong() shl this.header.pageShift)
    }

    /**
     * The [Header] or 0-Page of this HARE file. Contains important meta data about the HARE page file.
     *
     * @version 1.0
     * @author Ralph Gasser
     */
    protected inner class Header {
        /** A fixed [ByteBuffer] used to provide access to the header of this HARE file managed by this [DiskManager]. */
        val buffer: ByteBuffer = ByteBuffer.allocate(FILE_HEADER_SIZE)

        init {
            /* Read the file header. */
            this@DiskManager.fileChannel.read(this.buffer, 0L)
            this.buffer.rewind()

            /** Make necessary check on startup. */
            require(this.buffer.char == FILE_HEADER_IDENTIFIER[0]) { DataCorruptionException("HARE identifier missing in HARE page file (file: ${this@DiskManager.path.fileName}).") }
            require(this.buffer.char == FILE_HEADER_IDENTIFIER[1]) { DataCorruptionException("HARE identifier missing in HARE page file (file: ${this@DiskManager.path.fileName}).") }
            require(this.buffer.char == FILE_HEADER_IDENTIFIER[2]) { DataCorruptionException("HARE identifier missing in HARE page file (file: ${this@DiskManager.path.fileName}).") }
            require(this.buffer.char == FILE_HEADER_IDENTIFIER[3]) { DataCorruptionException("HARE identifier missing in HARE page file (file: ${this@DiskManager.path.fileName}).") }
            require(this.buffer.int == FileType.DEFAULT.ordinal)
            require(this.buffer.get() == FILE_HEADER_VERSION) { DataCorruptionException("File version mismatch in HARE page file (file: ${this@DiskManager.path.fileName}).") }
            require(this.buffer.int >= 10) { DataCorruptionException("Page shift mismatch in HARE page file (file: ${this@DiskManager.path.fileName}).") }
            this.buffer.get()
            require(this.buffer.long >= 0) { DataCorruptionException("Negative number of allocated pages found in HARE page file (file: ${this@DiskManager.path.fileName}).") }
            require(this.buffer.long >= 0) { DataCorruptionException("Negative number of freed pages found in HARE page file (file: ${this@DiskManager.path.fileName}).") }
        }

        /** The bit shift used to determine the [Page] size of the page file managed by this [DiskManager]. */
        val pageShift: Int = this.buffer.getInt(13)

        /** The [Page] size of the page file managed by this [DiskManager]. */
        val pageSize: Int = 1 shl this.pageShift

        /** Sets file sanity byte according to consistency status. */
        var isConsistent: Boolean
            get() = this.buffer.get(17) == FILE_CONSISTENCY_OK
            set(v) {
                if (v) {
                    this.buffer.put(17, FILE_CONSISTENCY_OK)
                } else {
                    this.buffer.put(17, FILE_SANITY_CHECK)
                }
            }

        /** Total number of [Page]s managed by this [DiskManager]. */
        var pages: Long
            get() = this.buffer.getLong(18)
            set(v) {
                this.buffer.putLong(18, v)
            }

        /** Total number of freed [Page]s managed by this [DiskManager]. */
        var freed: Int
            get() = this.buffer.getInt(26)
            set(v) {
                this.buffer.putInt(26, v)
            }

        /** CRC32C checksum for the HARE file. */
        var checksum: Long
            get() = this.buffer.getLong(30)
            set(v) {
                this.buffer.putLong(30, v)
            }

        /**
         * Flushes the content of this [Header] to disk.
         */
        fun flush() {
            this@DiskManager.fileChannel.write(this.buffer.rewind(), 0)
            this.buffer.rewind()
        }
    }
}