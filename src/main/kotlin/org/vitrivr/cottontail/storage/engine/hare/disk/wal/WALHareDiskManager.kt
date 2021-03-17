package org.vitrivr.cottontail.storage.engine.hare.disk.wal

import it.unimi.dsi.fastutil.longs.Long2ObjectFunction
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.model.basics.TransactionId
import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.LongStack
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.shared
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.zip.CRC32C
import kotlin.math.max

/**
 * The [WALHareDiskManager] facilitates reading and writing of [Page]s from/to the underlying disk storage. Only one
 * [HareDiskManager] can be opened per HARE file and it acquires an exclusive [FileLock] once created.
 *
 * As opposed to other [HareDiskManager] implementations, the [WALHareDiskManager] uses a write-ahead log (WAL) to make changes
 * to the underlying file. Upon commit or rollback, changes to the WAL are atomically transferred to the actual file. This
 * makes this implementation slower but offers some reliability circumstances that involve system crashes.
 *
 * @see HareDiskManager
 *
 * @version 1.4.0
 * @author Ralph Gasser
 */
class WALHareDiskManager(path: Path, lockTimeout: Long = 5000, private val preAllocatePages: Int = 32) : HareDiskManager(path, lockTimeout) {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(WALHareDiskManager::class.java)

        /** File suffix of a undo log file. */
        private const val HARE_UNDO_LOG_SUFFIX = "uwal"
    }

    /**
     * Reference to all ongoing [UndoLog]s.
     *
     * [UndoLog]s are created whenever a write starts and removed on either COMMIT or ROLLBACK.
     */
    private val undoLogs = Long2ObjectOpenHashMap<UndoLog>()

    init {
        if (!this.header.properlyClosed || this.header.isDirty) {
            /* Find open Redo WAL files. */
            val matcher = FileSystems.getDefault().getPathMatcher("glob:*.$HARE_UNDO_LOG_SUFFIX")
            Files.walk(this.path.parent, 1).use { stream ->
                stream.forEach { path ->
                    if (matcher.matches(path.fileName)) {
                        val tid = path.fileName.toString().split(".")[1].toLong()
                        val log = UndoLog(tid, path, this.lockTimeout)
                        this.undoLogs[log.tid] = log
                    }
                }
            }

            /* Check if there are dangling undo logs. */
            if (this.undoLogs.size > 0) {
                LOGGER.warn("HARE page file was found with unfinished transactions; starting recovery...")
                this.recover()
            } else if (!this.validate()) {
                throw DataCorruptionException("CRC32C checksum mismatch (file: ${this.path}, expected:${this.calculateChecksum()}, found: ${this.header.checksum}}).")
            }
        }

        /* Updates properly closed & dirty flag in header. */
        this.header.isDirty = false
        this.header.properlyClosed = false
        this.header.write(this.fileChannel, OFFSET_HEADER)
    }

    /**
     * Fetches the data identified by the given [PageId] into the given [Page] object thereby
     * replacing the content of that [Page].
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     * @param pageId [PageId] to fetch data for.
     * @param page [Page] to fetch data into. Its content will be updated.
     */
    override fun read(tid: TransactionId, pageId: PageId, page: HarePage) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "HARE page file read failed: Channel closed and cannot be used to read data (file: ${this.path})." }
            page.read(this.fileChannel, this.pageIdToOffset(pageId))
        }
    }

    /**
     * Fetches the data starting from the given [PageId] into the given [Page] objects thereby replacing the content of those [Page]s.
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     * @param pageId [PageId] to start fetching
     * @param pages [HarePage]s to fetch data into. Their content will be updated.
     */
    override fun read(tid: TransactionId, pageId: PageId, pages: Array<HarePage>) {
        this.closeLock.read {
            check(this.fileChannel.isOpen) { "HARE page file read failed: Channel closed and cannot be used to read data (file: ${this.path})." }
            val buffers = Array(pages.size) { pages[it].buffer.clear() }
            this.fileChannel.position(this.pageIdToOffset(pageId))
            this.fileChannel.read(buffers)
            buffers.forEach { b -> b.clear() }
        }
    }

    /**
     * Updates the page  with the given [PageId] with the content in the [HarePage].
     *
     * This change will be written to the [UndoLog].
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     * @param pageId [PageId] of the [Page] that should be updated
     * @param page [HarePage] the data the [Page] should be updated with.
     */
    override fun update(tid: TransactionId, pageId: PageId, page: HarePage) {
        this.closeLock.shared {
            check(this.fileChannel.isOpen) { "HARE page file write failed: Channel closed and cannot be used to write data (file: ${this.path})." }

            /* Obtain UndoLog object for transaction and lock it. */
            val log = this.getOrStartUndoLog(tid)
            log.logSnapshot(pageId)

            /* Update header. */
            this.header.isDirty = true

            /* Write changes to disk. */
            this.header.write(this.fileChannel, OFFSET_HEADER)
            page.write(this.fileChannel, this.pageIdToOffset(pageId))
        }
    }

    /**
     * Allocates new [HarePage]s in the HARE page file managed by this [DirectHareDiskManager].
     *
     * The method will first try to return a [PageId] from the [LongStack] for free [PageId]s,
     * if that [LongStack] has run empty, then new pages are physically allocated and the file
     * will grow by the number of pages specified in [DirectHareDiskManager.preAllocatePages].
     *
     * This change will be written to the [UndoLog].
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     * @return The [PageId] of the allocated [Page].
     */
    override fun allocate(tid: TransactionId): PageId {
        return this.closeLock.shared {
            check(this.fileChannel.isOpen) { "HARE page file write failed: Channel closed and cannot be used to write data (file: ${this.path})." }

            /* Obtain UndoLog object for transaction and lock it. */
            this.getOrStartUndoLog(tid)

            /* Pre-allocate pages if LongStack is empty. */
            if (this.freePageStack.entries == 0) {
                val nextPageId = this.header.maximumPageId + 1
                val preAllocatePageId = nextPageId + this.preAllocatePages
                this.fileChannel.write(EMPTY.clear(), (preAllocatePageId + 1) shl this.header.pageShift)
                for (pageId in preAllocatePageId downTo nextPageId) {
                    this.freePageStack.offer(pageId)
                }
            }

            /* Allocate PageId and adjust header. */
            val newPageId = this.freePageStack.pop()
            this.header.isDirty = true
            this.header.allocatedPages += 1
            this.header.maximumPageId = max(this.header.maximumPageId, newPageId)

            /* Write all changes to disk. */
            this.header.write(this.fileChannel, OFFSET_HEADER)
            this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)

            /* Return ID of next free page. */
            newPageId
        }
    }

    /**
     * Frees the page with the given [PageId] making space for new entries
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     * @param pageId The [PageId] of the page that should be freed.
     */
    override fun free(tid: TransactionId, pageId: PageId) {
        this.closeLock.read {
            /* Sanity checks. */
            check(this.fileChannel.isOpen) { "HARE page file write failed: Channel closed and cannot be used to write data (file: ${this.path})." }
            require(!this.freePageStack.contains(pageId)) { "HARE page file write failed: Page ID $pageId has already been freed for this HARE page file (file: ${this.path}, pages: ${this.pages})." }

            /* Obtain UndoLog object for transaction and log page snapshot. */
            val log = this.getOrStartUndoLog(tid)
            log.logSnapshot(pageId)

            /* Free page by adding page to free page stack. */
            this.header.isDirty = true
            if (!this.freePageStack.offer(pageId)) {
                this.header.danglingPages += 1
            }

            /* Decrement number of allocated pages. */
            this.header.allocatedPages -= 1

            /* Write all changes to disk. */
            this.header.write(this.fileChannel, OFFSET_HEADER)
            this.freePageStack.write(this.fileChannel, OFFSET_FREE_PAGE_STACK)
            this.fileChannel.write(FREED.flip(), this.pageIdToOffset(pageId))
        }
    }

    /**
     * Performs a commit of all pending changes by discarding the [UndoLog] file.
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     */
    override fun commit(tid: TransactionId) {
        this.closeLock.shared {
            check(this.fileChannel.isOpen) { "HARE page file commit failed: Channel closed and cannot be used to write data (file: ${this.path})." }

            /* Obtain UndoLog object for transaction and lock it. */
            val log = this.undoLogs[tid]
            if (log != null) {
                /* Update file header. */
                this.header.isDirty = false
                this.header.checksum = this.calculateChecksum()
                this.header.write(this.fileChannel, OFFSET_HEADER)
                this.fileChannel.force(true)

                /* Log successful commit. */
                log.logCommit()

                /* Remove log and delete file. */
                this.undoLogs.remove(tid)
                log.delete()
            }
        }
    }

    /**
     * Performs a rollback of all pending changes by applying the [UndoLog] file.
     *
     * @param tid The [TransactionId] of the transaction that performs the action.
     */
    override fun rollback(tid: TransactionId) = this.closeLock.shared {
        check(this.fileChannel.isOpen) { "HARE page file rollback failed: Channel closed and cannot be used to write data (file: ${this.path})." }

        /* Obtain UndoLog object for transaction and lock it. */
        val log = this.undoLogs[tid]
        if (log != null) {
            /* Logs abort. */
            log.logAbort()

            /* Apply undo log. */
            log.apply()

            /* Remove log and delete file. */
            this.undoLogs.remove(tid)
            log.delete()
        }
    }

    /**
     * Deletes the HARE page file backing this [WALHareDiskManager] and associated [UndoLog] files.
     *
     * Calling this method also closes the associated [FileChannel]s.
     */
    override fun delete() {
        this.undoLogs.values.forEach { it.delete() }
        super.delete()
    }

    /**
     * Closes the [UndoLog] associated with this [WALHareDiskManager].
     */
    override fun prepareClose() {
        /* Closes all open WALs. */
        if (this.undoLogs.size > 0) {
            LOGGER.warn("HARE page file is being closed while having ongoing transactions.")
            this.undoLogs.values.forEach {
                it.close()
            }
        }

        /* Sets properly closed flag. */
        this.header.properlyClosed = true
        this.header.write(this.fileChannel, OFFSET_HEADER)
    }


    /**
     * Obtains or starts an [UndoLog] for the given [TransactionId].
     *
     * @param tid [TransactionId] to obtain or start the [UndoLog] for.
     * @return [UndoLog] for [TransactionId]
     */
    private fun getOrStartUndoLog(tid: TransactionId): UndoLog = this.undoLogs.computeIfAbsent(tid, Long2ObjectFunction {
        val name = this@WALHareDiskManager.path.fileName.toString().split(".").first()
        UndoLog(tid, this@WALHareDiskManager.path.parent.resolve("$name.$tid.$HARE_UNDO_LOG_SUFFIX"), this@WALHareDiskManager.lockTimeout)
    })

    /**
     * Performs recovery based on [UndoLog].
     */
    private fun recover() = this.closeLock.shared {
        check(this.fileChannel.isOpen) { "HARE page file recovery failed: Channel closed and cannot be used to write data (file: ${this.path})." }
        for (log in this.undoLogs.values) {
            when (log.state) {
                WALState.COMMITTED -> {
                    LOGGER.info("HARE page file recovery: Removing dangling undo log for transaction ${log.tid}.")
                    log.delete()
                }
                WALState.ABORTED -> {
                    /* Apply undo log. */
                    LOGGER.info("HARE page file recovery: Rolling back aborted transaction ${log.tid}.")
                    log.apply()

                    /* Remove log and delete file. */
                    log.delete()
                }
                WALState.LOGGING -> {
                    /* Log presumed abort.*/
                    LOGGER.info("HARE page file recovery: Rolling back interrupted transaction ${log.tid} (presumed abort).")
                    log.logAbort()

                    /* Apply undo log. */
                    log.apply()

                    /* Remove log and delete file. */
                    log.delete()
                }
            }
        }
        this.undoLogs.clear()
    }

    /**
     * This is a [WriteAheadLog] implementation that captures the old (snapshot) version of each [HarePage]
     * that is written through this [WALHareDiskManager]. The [UndoLog] can be used to rollback changes,
     * in case a transaction fails.
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    internal inner class UndoLog(override val tid: TransactionId, path: Path, lockTimeout: Long = 5000L) : WriteAheadLog(path, lockTimeout) {

        /**
         * Logs a snapshot for a [PageId] and appends it to the log.
         *
         * @param pageId The [PageId] to snapshot.
         */
        @Synchronized
        fun logSnapshot(pageId: PageId) = this.closeLock.shared {
            check(this.fileChannel.isOpen) { "HARE Write Ahead Log (WAL) file log failed: Channel has been closed and cannot be used (name = ${this.path.fileName})." }
            check(!this.walHeader.state.isSealed) { "HARE Write Ahead Log (WAL) file log failed: Log has been sealed (name = ${this.path.fileName})." }
            require(pageId <= this@WALHareDiskManager.maximumPageId) { "HARE Write Ahead Log (WAL) file log failed: Page ID $pageId is out of bounds (name = ${this.path.fileName})." }

            /* Prepare log entry. */
            this.entry.sequenceNumber = this.walHeader.entries++
            this.entry.action = WALAction.UNDO_SNAPSHOT
            this.entry.pageId = pageId

            /* Write log entry + transfer page snapshot. */
            this.entry.write(this.fileChannel)
            val pageSize = this@WALHareDiskManager.pageSize.toLong()
            val transferred = this@WALHareDiskManager.fileChannel.transferTo(pageIdToOffset(pageId), pageSize, this.fileChannel)
            if (transferred != pageSize) {
                throw IOException("HARE Write Ahead Log (WAL) file log failed: Requested $pageSize bytes for snapshot but only received $transferred.")
            }
            this.walHeader.write(this.fileChannel, OFFSET_WAL_HEADER) /* Logged last. */
        }

        /**
         * Applies the [UndoLog] thus reverting all changes since the last checkpoint.
         */
        @Synchronized
        fun apply() = this.closeLock.shared {
            check(this.fileChannel.isOpen) { "HARE Write Ahead Log (WAL) undo failed: File has been closed and cannot be used for replay (name = ${this.path.fileName})." }

            /* Force-update header and compare checksum. */
            val pageSize = this@WALHareDiskManager.pageSize.toLong()
            this.walHeader.read(this.fileChannel, OFFSET_WAL_HEADER)
            if (this.calculateChecksum() != this.walHeader.checksum) {
                throw DataCorruptionException("HARE Write Ahead Log (WAL) undo failed: CRC32 checksum of data does not match checksum in header (name = ${this.path.fileName}).")
            }

            /* Read each entry, perform sanity checks and update CRC32. */
            for (seq in (this.walHeader.entries - 1L) downTo 0L) {
                /* Update FileChannels position. */
                this.fileChannel.position(this.walHeader.size + this@WALHareDiskManager.pageSize + seq * (this.entry.size + this@WALHareDiskManager.pageSize))

                /* Read entry and check sequence number. */
                this.entry.read(this.fileChannel)
                if (this.entry.sequenceNumber != seq) {
                    throw DataCorruptionException("HARE Write Ahead Log (WAL) undo failed: Sequence number mismatch at position $seq (name = ${this.path.fileName}).")
                }
                if (this.entry.action != WALAction.UNDO_SNAPSHOT) {
                    throw DataCorruptionException("HARE Write Ahead Log (WAL) undo failed: action mismatch at position $seq (name = ${this.path.fileName}).")
                }
                val transferred = this@WALHareDiskManager.fileChannel.transferFrom(this.fileChannel, this@WALHareDiskManager.pageIdToOffset(entry.pageId), pageSize)
                if (transferred != pageSize) {
                    throw DataCorruptionException("HARE Write Ahead Log (WAL) undo failed: truncated entry at position $seq (name = ${this.path.fileName}).")
                }
            }

            /* Transfer old header + long stack. */
            this.fileChannel.position(this.walHeader.size.toLong())
            this@WALHareDiskManager.fileChannel.transferFrom(this.fileChannel, 0L, pageSize)

            /* Force all changes and delete undo log. */
            this@WALHareDiskManager.fileChannel.force(true)

            /* Reset file channel's position to EOF. */
            this.fileChannel.position(this.fileChannel.size())

            /* Re-read file header. */
            if (!this@WALHareDiskManager.validate()) {
                throw DataCorruptionException("HARE Write Ahead Log (WAL) undo failed: Invalid CRC32C checksum for restored file (name = ${this.path.fileName}).")
            }
        }

        /**
         * Read the [WALHeader] and
         */
        override fun prepareOpen() {
            /* Initialize newly created UndoLog. */
            if (this.fileChannel.size() == 0L) {
                this.walHeader.init()
                this.walHeader.write(this.fileChannel)

                /* Copies state of WALHereDiskManager header (HareHeader + LongStack). */
                this@WALHareDiskManager.fileChannel.transferTo(0L, this@WALHareDiskManager.pageSize.toLong(), this.fileChannel)
            } else if (!this.valid()) {
                throw DataCorruptionException("HARE Write Ahead Log (WAL): CRC32 checksum of data does not match checksum in header.")
            }
        }

        /**
         * Re-calculates the [CRC32C] checksum for this [UndoLog].
         *
         * @return [Long] value of [CRC32C] checksum.
         */
        override fun calculateChecksum(): Long = this.closeLock.shared {
            /* Initialize required objects. */
            val crc32c = CRC32C()
            val page = HarePage(ByteBuffer.allocate(this@WALHareDiskManager.pageSize))

            /* Set file channel position to beginning for JOURNAL. */
            this.fileChannel.position(WALHeader.SIZE.toLong() + this@WALHareDiskManager.pageSize)

            /* Read each entry, perform sanity checks and update CRC32. */
            for (seq in 0L until this.walHeader.entries) {
                /* Read entry and check sequence number. */
                this.entry.read(this.fileChannel)
                if (this.entry.sequenceNumber != seq) {
                    throw DataCorruptionException("HARE Write Ahead Log (WAL) sequence number mismatch at position $seq (name = ${this.path.fileName}).")
                }
                if (this.entry.action != WALAction.UNDO_SNAPSHOT) {
                    throw DataCorruptionException("HARE Write Ahead Log (WAL) action mismatch at position $seq; UNDO_SNAPSHOT expected (name = ${this.path.fileName}).")
                }
                page.read(this.fileChannel)
                crc32c.update(page.buffer)
            }

            /* Reset file channel position to EOF. */
            this.fileChannel.position(this.fileChannel.size())
            return crc32c.value
        }
    }
}