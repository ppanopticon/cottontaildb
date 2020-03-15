package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.wal

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.DataCorruptionException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.*
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Constants.FILE_HEADER_VERSION
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Constants.FILE_CONSISTENCY_OK
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Constants.PAGE_DATA_SIZE_BYTES

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
open class WriteAheadLog(val path: Path, val lockTimeout: Long = 5000L, private var pageIdStart: Long = 0L) : AutoCloseable {


    companion object {
        private const val WAL_ACTION_SIZE_BYTES = 4
        private const val WAL_PAGEID_SIZE_BYTES = 8
        val WAL_PAGE_SIZE = WAL_ACTION_SIZE_BYTES + WAL_PAGEID_SIZE_BYTES + PAGE_DATA_SIZE_BYTES
    }

    /** [FileChannel] used to write to this [WriteAheadLog]*/
    private val fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SPARSE, StandardOpenOption.DSYNC)

    /** Acquire lock on [WriteAheadLog] file. */
    private val fileLock = FileUtilities.acquireFileLock(this.fileChannel, this.lockTimeout)

    /** Internal [ByteBuffer] used for writing WAL entries. */
    private val buffer: ByteBuffer = ByteBuffer.allocate(WAL_PAGE_SIZE)

    /** Accessor to the [Header] of the HARE [WriteAheadLog] file. */
    private val header = Header(this.fileChannel.size() == 0L)

    /** The [CRC32C] object used to calculate the CRC32 checksum of this [WriteAheadLog]. */
    private val crc32 = CRC32C()

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
    fun append(action: WALAction, page: Page) {
        check(this.fileChannel.isOpen) { "HARE Write Ahead Log (WAL) file for {${this.path}} has been closed and cannot be used for append." }

        /* Prepare buffer and update checksum. */
        if (action == WALAction.APPEND) {
            this.header.maxPageId += 1
            page.id = this.header.maxPageId
        }
        this.buffer.rewind().putInt(action.ordinal).putLong(page.id).put(page.data).rewind()
        this.crc32.update(this.buffer)

        /* Update header of WAL file. */
        this.header.entries += 1
        this.header.checksum = crc32.value

        /* Write to WAL file. */
        this.fileChannel.write(this.buffer.rewind(), this.header.entries * WAL_PAGE_SIZE)
        this.header.flush()
    }

    /**
     * Replays this [WriteAheadLog] thus transferring all changes into the given destination.
     *
     * @param consumer A function that consumes the [WriteAheadLog] entry and return true on success.
     */
    @Synchronized
    fun replay(consumer: (WALAction, Long, ByteBuffer) -> Boolean) {
        check(this.fileChannel.isOpen) { "HARE Write Ahead Log (WAL) file for {${this.path}} has been closed and cannot be used for replay." }

        for (i in (this.header.transferred + 1)..this.header.entries) {
            this.fileChannel.read(this.buffer.rewind(), i * WAL_PAGE_SIZE)
            val action = WALAction.values()[this.buffer.rewind().int]
            val pageId = this.buffer.long

            /* Execute consumer; if it returns true, update WAL header. */
            if (consumer(action, pageId, this.buffer)) {
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
    private inner class Header(new: Boolean) {
        /** A fixed 4096 byte [ByteBuffer] used to provide access to the header of this HARE file managed by this [DiskManager]. */
        private val buffer: ByteBuffer = ByteBuffer.allocateDirect(WAL_PAGE_SIZE)

        init {
            if (new) {
                /* Initialize new WAL file header. */
                this.buffer.putChar(Constants.FILE_HEADER_IDENTIFIER[0])             /* 0: Identifier H. */
                this.buffer.putChar(Constants.FILE_HEADER_IDENTIFIER[1])             /* 2: Identifier A. */
                this.buffer.putChar(Constants.FILE_HEADER_IDENTIFIER[2])             /* 4: Identifier R. */
                this.buffer.putChar(Constants.FILE_HEADER_IDENTIFIER[3])             /* 6: Identifier E. */
                this.buffer.putInt(FileType.DEFAULT.ordinal)                         /* 8: Type of HARE file. */
                this.buffer.put(FILE_HEADER_VERSION)                                 /* 12: Version of the HARE format. */
                this.buffer.put(FILE_CONSISTENCY_OK)                                      /* 13: Sanity byte; 0 if file was properly closed, 1 if not.  */
                this.buffer.putLong(0L)                                        /* 14: Number of entries in this Write Ahead Log. */
                this.buffer.putLong(0L)                                        /* 22: Number of transferred entries in this Write Ahead Log. */
                this.buffer.putLong(this@WriteAheadLog.pageIdStart)                  /* 30: Last Page ID allocated (set by caller). */
                this.buffer.putLong(0L)                                        /* 38: CRC32 checksum for HARE file. */
            } else {
                /** Make necessary check on startup. */
                require(this.buffer.char == Constants.FILE_HEADER_IDENTIFIER[0]) { DataCorruptionException("HARE identifier missing in HARE WAL file ${this@WriteAheadLog.path.fileName}.") }
                require(this.buffer.char == Constants.FILE_HEADER_IDENTIFIER[1]) { DataCorruptionException("HARE identifier missing in HARE WAL file ${this@WriteAheadLog.path.fileName}.") }
                require(this.buffer.char == Constants.FILE_HEADER_IDENTIFIER[2]) { DataCorruptionException("HARE identifier missing in HARE WAL file ${this@WriteAheadLog.path.fileName}.") }
                require(this.buffer.char == Constants.FILE_HEADER_IDENTIFIER[3]) { DataCorruptionException("HARE identifier missing in HARE WAL file ${this@WriteAheadLog.path.fileName}.") }
                require(this.buffer.int == FileType.WAL.ordinal) { DataCorruptionException("HARE file type mismatch in HARE WAL file ${this@WriteAheadLog.path.fileName}.") }
                require(this.buffer.get() == FILE_HEADER_VERSION) { DataCorruptionException("HARE file version mismatch in HARE file ${this@WriteAheadLog.path.fileName}.") }
                require(this.entries >= 0) { DataCorruptionException("Negative number of entries found in HARE WAL file ${this@WriteAheadLog.path.fileName}.") }
                require(this.transferred >= 0) { DataCorruptionException("Negative number of transferred entries found in HARE WAL file ${this@WriteAheadLog.path.fileName}.") }
                require(this.maxPageId >= 0) { DataCorruptionException("Negative number for last PageId found in HARE file ${this@WriteAheadLog.path.fileName}.") }

                val pageBuffer = ByteBuffer.allocateDirect(WAL_PAGE_SIZE)
                for (i in 1..this.entries) {
                    this@WriteAheadLog.fileChannel.read(pageBuffer.rewind(), i * WAL_PAGE_SIZE)
                    this@WriteAheadLog.crc32.update(pageBuffer)
                    require(crc32.value == this.checksum) { DataCorruptionException("CRC32C checksum not correct (expected: ${this.checksum}, found: ${crc32.value}) in HARE WAL file ${this@WriteAheadLog.path.fileName}.") }
                }

                /* Update last page id. */
                this@WriteAheadLog.pageIdStart = this.maxPageId
            }

            /* Flush changes to disk. */
            this.flush()
        }


        /** Total number of entries contained in this [WriteAheadLog] file. */
        var entries: Long
            get() = this.buffer.getLong(14)
            set(v) {
                this.buffer.putLong(14, v)
            }

        /** Total number of entries contained in this [WriteAheadLog] file. */
        var transferred: Long
            get() = this.buffer.getLong(22)
            set(v) {
                this.buffer.putLong(22, v)
            }

        /** The last [PageId] that was allocated; used to keep track of [PageId] for newly allocated [Page]s. */
        var maxPageId: Long
            get() = this.buffer.getLong(30)
            set(v) {
                this.buffer.putLong(30, v)
            }

        /** Total number of entries contained in this [WriteAheadLog] file. */
        var checksum: Long
            get() = this.buffer.getLong(38)
            set(v) {
                this.buffer.putLong(38, v)
            }

        /**
         * Flushes the content of this [Header] to disk.
         */
        fun flush() = this@WriteAheadLog.fileChannel.write(this.buffer.rewind(), 0)
    }
}