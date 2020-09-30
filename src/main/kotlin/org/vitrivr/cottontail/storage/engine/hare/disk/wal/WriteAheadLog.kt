package org.vitrivr.cottontail.storage.engine.hare.disk.wal

import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.disk.DataPage
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager.Companion.FILE_CONSISTENCY_OK
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager.Companion.FILE_HEADER_IDENTIFIER
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager.Companion.FILE_HEADER_VERSION
import org.vitrivr.cottontail.storage.engine.hare.disk.FileType
import org.vitrivr.cottontail.storage.engine.hare.disk.FileUtilities
import org.vitrivr.cottontail.utilities.extensions.exclusive

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.zip.CRC32C

/**
 * A file used for write-ahead logging. It allows for all the basic operations supported by [DiskManager]s. The series
 * of operations executed by this [WriteAheadLog] can then be replayed.
 *
 * @see WALDiskManager
 *
 * @author Ralph Gasser
 * @version 1.0
 */
open class WriteAheadLog(val path: Path, val lockTimeout: Long = 5000L) : AutoCloseable {

    companion object {
        /** Prefix used in the [WriteAheadLog] for each entry. */
        private const val WAL_HEADER_SIZE = 64

        /** Prefix used in the [WriteAheadLog] for each entry. */
        private const val WAL_PREFIX_BYTES = 12

        /**
         * Creates a new page file in the HARE format.
         *
         * @param path [Path] under which to create the page file.
         */
        fun create(path: Path, maxPageId: PageId, pageShift: Int = 12) {
            /* Prepare header data for page file in the HARE format. */
            val data: ByteBuffer = ByteBuffer.allocateDirect(WAL_HEADER_SIZE)
            data.putChar(FILE_HEADER_IDENTIFIER[0])             /* 0: Identifier H. */
            data.putChar(FILE_HEADER_IDENTIFIER[1])             /* 2: Identifier A. */
            data.putChar(FILE_HEADER_IDENTIFIER[2])             /* 4: Identifier R. */
            data.putChar(FILE_HEADER_IDENTIFIER[3])             /* 6: Identifier E. */
            data.putInt(FileType.WAL.ordinal)               /* 8: Type of HARE file. */
            data.put(FILE_HEADER_VERSION)                       /* 12: Version of the HARE format. */
            data.putInt(pageShift)                              /* 13: Size of a HARE page. Indicated as bit shift. */
            data.put(FILE_CONSISTENCY_OK)                       /* 17: Sanity byte; exact semantic depends on implementation. */
            data.putLong(0L)                              /* 18: Page counter; number of pages. */
            data.putLong(0L)                              /* 26: Page counter; number of transferred pages. */
            data.putLong(maxPageId)                             /* 34: Maximum page ID. */
            data.putLong(0L)                              /* 42: CRC32 checksum for HARE file. */

            /** Write data to file and close. */
            val channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, StandardOpenOption.SYNC, StandardOpenOption.SPARSE)
            channel.write(data.rewind())
            channel.close()
        }
    }

    /** [FileChannel] used to write to this [WriteAheadLog]*/
    private val fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SPARSE, StandardOpenOption.DSYNC)

    /** Acquire lock on [WriteAheadLog] file. */
    private val fileLock = FileUtilities.acquireFileLock(this.fileChannel, this.lockTimeout)

    /** Accessor to the [Header] of the HARE [WriteAheadLog] file. */
    private val header = Header()

    /** The [CRC32C] object used to calculate the CRC32 checksum of this [WriteAheadLog]. */
    private val crc32 = CRC32C()

    /** A local [ByteBuffer] for data allocation. Can hold up to on WAL page. */
    private val allocationBuffer = ByteBuffer.allocateDirect(this.header.walPageSize)

    /** The maximum [PageId] 'seen' by this [WriteAheadLog].  */
    val maxPageId
        get() = this.header.maxPageId

    /**
     * Appends a [WALAction] and the associated data to this [WriteAheadLog].
     *
     * @param action The actual type of [WALAction] that was performed.
     * @param page The [Page] that was written.
     */
    @Synchronized
    fun append(action: WALAction, id: PageId? = null, page: DataPage? = null) {
        check(this.fileChannel.isOpen) { "HARE Write Ahead Log (WAL) file for {${this.path}} has been closed and cannot be used for append." }

        /* Load buffer with data and update checksum. */
        this.allocationBuffer.clear()
        when (action) {
            WALAction.APPEND -> {
                this.header.maxPageId += 1
                this.allocationBuffer.putInt(action.ordinal).putLong(this.header.maxPageId)
                if (page != null) {
                    page.lock.exclusive {
                        this.allocationBuffer.put(page._data)
                        page._data.clear()
                    }
                } else {
                    repeat(this.header.pageSize) { this.allocationBuffer.put(0) }
                }
            }
            WALAction.UPDATE -> {
                require (id != null) { "HARE Write Ahead Log (WAL) UPDATE action requires a non-null page ID." }
                this.allocationBuffer.putInt(action.ordinal).putLong(id)
                if (page != null) {
                    page.lock.exclusive {
                        this.allocationBuffer.put(page._data)
                        page._data.clear()
                    }
                } else {
                    repeat(this.header.pageSize) { this.allocationBuffer.put(0) }
                }
            }
            WALAction.FREE -> {
                require (id != null) { "HARE Write Ahead Log (WAL) FREE action requires a non-null page ID." }
                this.allocationBuffer.putInt(action.ordinal).putLong(id)
                repeat(this.header.pageSize) { this.allocationBuffer.put(0) }
            }
        }

        /** Calculate CRC32 checksum. */
        this.allocationBuffer.flip()
        this.crc32.update(this.allocationBuffer)

        /* Write to WAL file. */
        this.allocationBuffer.flip()
        this.fileChannel.write(this.allocationBuffer)

        /* Update header of WAL file. */
        this.header.entries += 1
        this.header.checksum = crc32.value
        this.header.flush()
    }

    /**
     * Replays this [WriteAheadLog] thus transferring all changes into the given destination.
     *
     * @param consumer A function that consumes the [WriteAheadLog] entry and return true on success.
     */
    @Synchronized
    fun replay(consumer: (WALAction, PageId, FileChannel) -> Boolean) {
        check(this.fileChannel.isOpen) { "HARE Write Ahead Log (WAL) file for {${this.path}} has been closed and cannot be used for replay." }

        /* Initialize ByteBuffer to read prefixes.*/
        val prefixBuffer = ByteBuffer.allocateDirect(WAL_PREFIX_BYTES)

        /* Initialize FileChannel position. */
        this.fileChannel.position(WAL_HEADER_SIZE.toLong())

        for (i in this.header.transferred..this.header.entries) {
            this.fileChannel.read(prefixBuffer.rewind())
            prefixBuffer.rewind()

            val action = WALAction.values()[prefixBuffer.int]
            val pageId = prefixBuffer.long

            /* Execute consumer; if it returns true, update WAL header. */
            if (consumer(action, pageId, this.fileChannel)) {
                this.header.transferred ++
                this.header.flush()
            }
        }
    }

    @Synchronized
    override fun close() {
        if (this.fileChannel.isOpen) {
            this.fileLock.release()
            this.fileChannel.close()
        }
    }

    /**
     * Deletes this [WriteAheadLog] file. Calling this method also closes the associated [FileChannel].
     */
    @Synchronized
    fun delete() {
        this.close()
        Files.delete(this.path)
    }

    /**
     * The [Header] or 0-Page of this HARE file.
     *
     * @version 1.0
     * @author Ralph Gasser
     */
    private inner class Header {
        /** A [ByteBuffer] used to provide access to the header of this HARE WAL file. */
        private val buffer: ByteBuffer = ByteBuffer.allocateDirect(WAL_HEADER_SIZE)

        init {
            /* Read the file header. */
            this@WriteAheadLog.fileChannel.read(this.buffer)
            this.buffer.rewind()

            /** Make necessary check on startup. */
            if(this.buffer.char != FILE_HEADER_IDENTIFIER[0]) { throw DataCorruptionException("HARE identifier missing in HARE WAL file (file: ${this@WriteAheadLog.path.fileName}).") }
            if(this.buffer.char != FILE_HEADER_IDENTIFIER[1]) { throw DataCorruptionException("HARE identifier missing in HARE WAL file (file: ${this@WriteAheadLog.path.fileName}).") }
            if(this.buffer.char != FILE_HEADER_IDENTIFIER[2]) { throw DataCorruptionException("HARE identifier missing in HARE WAL file (file: ${this@WriteAheadLog.path.fileName}).") }
            if(this.buffer.char != FILE_HEADER_IDENTIFIER[3]) { throw DataCorruptionException("HARE identifier missing in HARE WAL file (file: ${this@WriteAheadLog.path.fileName}).") }
            if(this.buffer.int != FileType.WAL.ordinal) { throw DataCorruptionException("HARE file type mismatch in HARE WAL file (file: ${this@WriteAheadLog.path.fileName}).") }
            if(this.buffer.get() != FILE_HEADER_VERSION) { throw DataCorruptionException("HARE file version mismatch in HARE file (file: ${this@WriteAheadLog.path.fileName}).") }
            if(this.buffer.int < 12) { throw DataCorruptionException("Page shift mismatch in HARE page file (file: ${this@WriteAheadLog.path.fileName}).") }
            this.buffer.get()
            if(this.buffer.long < 0) { throw DataCorruptionException("Negative number of entries found in HARE WAL file (file: ${this@WriteAheadLog.path.fileName}).") }
            if(this.buffer.long < 0) { throw DataCorruptionException("Negative number of transferred entries found in HARE WAL file (file: ${this@WriteAheadLog.path.fileName}).") }
            if(this.buffer.long < 0) { throw DataCorruptionException("Negative number for last page ID found in HARE file (file: ${this@WriteAheadLog.path.fileName}).") }

            val expectedPageSize = WAL_PREFIX_BYTES + (1 shl this.buffer.getInt(13))
            val pageBuffer = ByteBuffer.allocateDirect(expectedPageSize)
            for (i in 1..this.entries) {
                this@WriteAheadLog.fileChannel.read(pageBuffer.rewind())
                this@WriteAheadLog.crc32.update(pageBuffer)
                require(crc32.value == this.checksum) { DataCorruptionException("CRC32C checksum not correct (expected: ${this.checksum}, found: ${crc32.value}) in HARE WAL file ${this@WriteAheadLog.path.fileName}.") }
            }

            /* Flush changes to disk. */
            this.flush()
        }

        /** The bit shift used to determine the [DataPage] size of the page file managed by this [DiskManager]. */
        val pageShift: Int = this.buffer.getInt(13)

        /** The bit shift used to determine the [DataPage] size of the page file managed by this [DiskManager]. */
        val pageSize: Int = (1 shl this.pageShift)

        /** Size of an individual [WriteAheadLog] page in bytes. */
        val walPageSize = this.pageSize + WAL_PREFIX_BYTES

        /** Total number of entries contained in this [WriteAheadLog] file. */
        var entries: Long
            get() = this.buffer.getLong(18)
            set(v) {
                this.buffer.putLong(18, v)
            }

        /** Total number of entries contained in this [WriteAheadLog] file. */
        var transferred: Long
            get() = this.buffer.getLong(26)
            set(v) {
                this.buffer.putLong(26, v)
            }

        /** The last [Int] that was allocated; used to keep track of [PageId] for newly allocated [DataPage]s. */
        var maxPageId: Long
            get() = this.buffer.getLong(34)
            set(v) {
                this.buffer.putLong(34, v)
            }

        /** Total number of entries contained in this [WriteAheadLog] file. */
        var checksum: Long
            get() = this.buffer.getLong(42)
            set(v) {
                this.buffer.putLong(42, v)
            }

        /**
         * Flushes the content of this [Header] to disk.
         */
        fun flush() = this@WriteAheadLog.fileChannel.write(this.buffer.rewind(), 0)
    }
}