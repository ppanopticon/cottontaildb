package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.storage.basics.MemorySize
import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.DataCorruptionException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.FileLockException
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.PageIdOutOfBoundException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CRC32C

/**
 * The [DiskManager] facilitates reading and writing of [Page]s from/to the underlying disk storage. Only one
 * [DiskManager] can be opened per HARE file and it acquires an exclusive [FileLock] once created.
 *
 * The [DiskManager] only transfers bytes from and to [Page]s. The management of [Page]s is handled by the [BufferPool].
 *
 * @see BufferPool
 *
 * @version 1.0
 * @author Ralph Gasser
 */
abstract class DiskManager(val path: Path, val lockTimeout: Long) : AutoCloseable {
    companion object {
        /** Identifier of every HARE file. */
        val FILE_HEADER_IDENTIFIER = charArrayOf('H', 'A', 'R', 'E')

        /** Version of the HARE file. */
        const val FILE_HEADER_VERSION = 1.toByte()

        /** Flag for when the file's sanity can be considered OK (i.e. it was closed correctly). */
        const val FILE_SANITY_OK = 0.toByte()

        /** Flag for when the file's sanity requires a check (i.e. it was not closed correctly). */
        const val FILE_SANITY_CHECK = 1.toByte()

        /** Size of the HARE file header, which is located on [Page] 0 in the file. */
        const val FILE_HEADER_SIZE_BYTES = 36

        /**
         * Initializes a new HARE file. Fails, if such a file already exists.
         *
         * @param path [Path] pointing to the new HARE file.
         */
        fun create(path: Path, lockTimeout: Long = 5000) {
            /* Generate file channel and acquire lock. */
            val fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SYNC, StandardOpenOption.SPARSE, StandardOpenOption.CREATE_NEW)
            val lock = acquireFileLock(fileChannel, lockTimeout)

            /* Initialize file. */
            val buffer = ByteBuffer.allocate(Page.Constants.PAGE_DATA_SIZE_BYTES)
            buffer.putChar(FILE_HEADER_IDENTIFIER[0])             /* 0: Identifier H. */
            buffer.putChar(FILE_HEADER_IDENTIFIER[1])             /* 2: Identifier A. */
            buffer.putChar(FILE_HEADER_IDENTIFIER[2])             /* 4: Identifier R. */
            buffer.putChar(FILE_HEADER_IDENTIFIER[3])             /* 6: Identifier E. */
            buffer.putInt(FileType.DEFAULT.ordinal)               /* 8: Type of HARE file. */
            buffer.put(FILE_HEADER_VERSION)             /* 12: Version of the HARE format. */
            buffer.put(FILE_SANITY_OK)                  /* 13: Sanity byte; 0 if file was properly closed, 1 if not.  */
            buffer.putLong(0L)                    /* 14: Page counter; number of pages. */
            buffer.putInt(0)                      /* 22: Page counter; number of freed pages. */
            buffer.putLong(0L)                    /* 26: CRC32 checksum for HARE file. */
            fileChannel.write(buffer.rewind())

            /* Unlock file and close channel. */
            lock.release()
            fileChannel.close()
        }

        /**
         * Tries to acquire a file lock of this [FileChannel] and returns it.
         *
         * @param timeout The amount of milliseconds to wait for lock.
         * @return lock The [FileLock] acquired.
         */
        @JvmStatic
        protected fun acquireFileLock(channel: FileChannel, timeout: Long) : FileLock {
            val start = System.currentTimeMillis()
            do {
                try {
                    val lock = channel.tryLock()
                    if (lock != null) {
                        return lock
                    } else {
                        Thread.sleep(100)
                    }
                } catch (e: IOException) {
                    throw FileLockException("Could not open DiskManager for HARE file: failed to acquire file lock due to IOException.", e)
                }
            } while (System.currentTimeMillis() - start < timeout)
            throw FileLockException("Could not open DiskManager for HARE file: failed to acquire file lock due to timeout (time elapsed > ${timeout}ms).")
        }
    }

    /** The [FileChannel] used to access the file managed by this [DiskManager]. */
    protected val fileChannel: FileChannel = FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC, StandardOpenOption.SPARSE)

    /** Acquires an exclusive [FileLock] for file underlying this [FileChannel]. Makes sure, that no other process uses the same HARE file. */
    protected val fileLock = acquireFileLock(this.fileChannel, this.lockTimeout)

    /** Flag indicating, whether this [DiskManager] was closed. */
    protected val closed: AtomicBoolean = AtomicBoolean(false)

    /** Accessor to the [Header] of the HARE file managed by this [DiskManager]. */
    protected val header = Header()

    /** Returns the size of the HARE file managed by this [DiskManager]. */
    val size
        get() = MemorySize(this.fileChannel.size().toDouble(), Units.BYTE)

    /** Number of [Page]s held by the HARE file managed by this [DiskManager]. */
    val pages
        get() = this.header.pages

    /**
     * Fetches the data identified by the given [PageId] into the given [Page] object thereby replacing the content of that [Page].
     *
     * @param id [PageId] to fetch data for.
     * @param page [Page] to fetch data into. Its content will be updated.
     */
    abstract fun read(id: PageId, page: Page)

    /**
     * Updates the [Page] in the HARE file managed by this [DirectDiskManager].
     *
     * @param page [Page] to update.
     */
    abstract fun update(page: Page)

    /**
     * Allocates new [Page] in the HARE file managed by this [DirectDiskManager].
     *
     * @param page [Page] to append. Its [PageId] and flags will be updated.
     */
    abstract fun allocate(page: Page)

    /**
     * Frees the [Page] identified by the given [PageId].
     *
     * @param pageId The [PageId] of the [Page] that should be freed.
     */
    abstract fun free(pageId: PageId)

    /**
     * Commits all changes made through this [DiskManager].
     */
    abstract fun commit()

    /**
     * Rolls back all changes made through this [DiskManager].
     */
    abstract fun rollback()

    /**
     * Closes this [DiskManager], releasing the underlying file.
     */
    override fun close() {
        this.header.deinit()
        if (this.closed.compareAndSet(false, true)) {
            this.fileLock.release()
            this.fileChannel.close()
        }
    }

    /**
     * Converts the given [PageId] to an offset into the file managed by this [DirectDiskManager]. Calling this method
     * also makes necessary sanity checks regarding the file's channel status and pageId bounds.
     *
     * @param pageId The [PageId] to translate to a position.
     * @return The offset into the file.
     */
    protected fun pageIdToPosition(pageId: PageId): Long {
        if (this.closed.get()) throw IllegalStateException("DiskManager for {${this.path}} was closed and cannot be used to access data.")
        if (pageId > this.header.pages || pageId < 1) throw PageIdOutOfBoundException(pageId, this)
        return pageId shl Page.Constants.PAGE_BIT_SHIFT
    }

    /**
     * The [Header] or 0-Page of this HARE file.
     *
     * @version 1.0
     * @author Ralph Gasser
     */
    protected inner class Header {
        /** A fixed 4096 byte [ByteBuffer] used to provide access to the header of this HARE file managed by this [DiskManager]. */
        private val buffer: ByteBuffer = ByteBuffer.allocateDirect(Page.Constants.PAGE_DATA_SIZE_BYTES)

        init {
            this@DiskManager.fileChannel.read(this.buffer, 0L)
            this.buffer.rewind()

            /* Checks the header for its validity. */
            this.init()
        }

        /** Total number of [Page]s managed by this [DiskManager]. */
        var pages: Long
            get() {
                return this.buffer.getLong(14)
            }

            set(v) {
                this.buffer.putLong(14, v)
            }

        /** Total number of freed [Page]s managed by this [DiskManager]. */
        var freed: Int
            get() {
                return this.buffer.getInt(22)
            }

            set(v) {
                this.buffer.putInt(22, v)
            }

        /** CRC32C checksum for the HARE file. */
        val checksum: Long
            get() {
                return this.buffer.getLong(26)
            }

        /** Total number of used [Page]s. */
        val used: Long
            get() {
                return this.buffer.getLong(10) - this. buffer.getInt(18)
            }

        /**
         * Initializes this [Header], by doing necessary checks and updating the sanity flag.
         */
        @Synchronized
        fun init() {
            /** Make necessary check on startup. */
            require(this.buffer.char == FILE_HEADER_IDENTIFIER[0]) { DataCorruptionException("HARE identifier missing in HARE file ${this@DiskManager.path.fileName}.") }
            require(this.buffer.char == FILE_HEADER_IDENTIFIER[1]) { DataCorruptionException("HARE identifier missing in HARE file ${this@DiskManager.path.fileName}.") }
            require(this.buffer.char == FILE_HEADER_IDENTIFIER[2]) { DataCorruptionException("HARE identifier missing in HARE file ${this@DiskManager.path.fileName}.") }
            require(this.buffer.char == FILE_HEADER_IDENTIFIER[3]) { DataCorruptionException("HARE identifier missing in HARE file ${this@DiskManager.path.fileName}.") }
            require(this.buffer.int == FileType.DEFAULT.ordinal)
            require(this.buffer.get() == FILE_HEADER_VERSION) { DataCorruptionException("HARE file version is incorrect in HARE file ${this@DiskManager.path.fileName}.") }
            require(this.pages >= 0) { DataCorruptionException("Negative number of allocated pages found in HARE file ${this@DiskManager.path.fileName}.") }
            require(this.freed >= 0) { DataCorruptionException("Negative number of freed pages found in HARE file ${this@DiskManager.path.fileName}.") }

            if (this.buffer.get() != FILE_SANITY_OK) {
                val page = Page(ByteBuffer.allocateDirect(Page.Constants.PAGE_DATA_SIZE_BYTES))
                val crc32 = CRC32C()
                for (i in 1..this.pages) {
                    this@DiskManager.read(i, page)
                    crc32.update(page.data)
                    require(crc32.value == this.checksum) { DataCorruptionException("CRC32C checksum not correct (expected: ${this.checksum}, found: ${crc32.value}) of HARE file ${this@DiskManager.path.fileName}.") }
                }
            }

            /* Updates sanity flag. */
            this.buffer.put(13, FILE_SANITY_CHECK)

            /* Update sanity flag to FILE_SANITY_CHECK at position 9. If file isn't closed properly, this flag will remain*/
            this.flush()
        }

        /**
         * De-initializes this [Header], by updating the sanity flag and the CRC32 checksum
         */
        @Synchronized
        fun deinit() {
            val page = Page(ByteBuffer.allocateDirect(Page.Constants.PAGE_DATA_SIZE_BYTES))
            val crc32 = CRC32C()
            for (i in 1..this.pages) {
                this@DiskManager.read(i, page)
                crc32.update(page.data)
            }

            /* Update the sanity byte. */
            this.buffer.put(13, FILE_SANITY_OK)
            this.buffer.putLong(26, crc32.value)

            /* Flush the [Header]. */
            this.flush()
        }

        /**
         * Flushes the content of this [Header] to disk.
         */
        fun flush() {
            this@DiskManager.fileChannel.write(this.buffer, 0)
            this.buffer.rewind()
        }
    }
}