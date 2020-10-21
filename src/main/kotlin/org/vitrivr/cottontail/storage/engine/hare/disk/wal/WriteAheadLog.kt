package org.vitrivr.cottontail.storage.engine.hare.disk.wal

import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.disk.DataPage
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.DiskManager.Companion.FILE_HEADER_IDENTIFIER
import org.vitrivr.cottontail.storage.engine.hare.disk.FileUtilities
import org.vitrivr.cottontail.utilities.extensions.exclusive
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.zip.CRC32C

/**
 * A file used for write-ahead logging. It allows for all the basic operations supported by
 * [DiskManager]s. The series of operations executed by this [WriteAheadLog] can then be replayed.
 *
 * @see WALDiskManager
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
open class WriteAheadLog(val path: Path, val lockTimeout: Long = 5000L) : AutoCloseable {

    companion object {
        /** Size of the [WriteAheadLog.Header] in bytes. */
        private const val OFFSET_HEADER = 0L

        /** Size of the [WriteAheadLog.Header] in bytes. */
        private const val WAL_HEADER_SIZE = 128

        /** Size of a [WriteAheadLog] entry. */
        private const val WAL_ENTRY_SIZE = 12

        /**
         * Creates a new page file in the HARE format.
         *
         * @param path [Path] under which to create the page file.
         */
        fun create(path: Path, maxPageId: PageId, pageShift: Int = 12) {
            /* Prepare header data for page file in the HARE format. */
            val header = WALHeader(ByteBuffer.allocateDirect(WAL_HEADER_SIZE)).init()

            /** Write data to file and close. */
            val channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, StandardOpenOption.SYNC, StandardOpenOption.SPARSE)
            header.write(channel, OFFSET_HEADER)
            channel.close()
        }
    }

    /** [FileChannel] used to write to this [WriteAheadLog]*/
    private val fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SPARSE, StandardOpenOption.DSYNC)

    /** Acquire lock on [WriteAheadLog] file. */
    private val fileLock = FileUtilities.acquireFileLock(this.fileChannel, this.lockTimeout)

    /** Reference to the [WALHeader] of the HARE [WriteAheadLog] file. */
    private val header = WALHeader(ByteBuffer.allocateDirect(WAL_HEADER_SIZE))

    /** The [CRC32C] object used to calculate the CRC32 checksum of this [WriteAheadLog]. */
    private val crc32 = CRC32C()

    /**
     * Appends a [WALAction] and the associated data to this [WriteAheadLog].
     *
     * @param action The actual type of [WALAction] that was performed.
     * @param pageId [PageId] affected by the [WALAction]
     * @param page [DataPage] containing data affected by the [WALAction]
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
        val prefixBuffer = ByteBuffer.allocateDirect(WAL_ENTRY_SIZE)

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
}