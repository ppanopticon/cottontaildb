package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.storage.basics.MemorySize
import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The [DiskManager] facilitates reading and writing of [Page]s from/to the underlying disk storage.
 * Only one [DiskManager] can be opened per HARE file and it acquires an exclusive [FileLock] once created.
 *
 * The [DiskManager] only transfers bytes from and to [Page]s. The management of [Page]s is handled by the [BufferPool].
 *
 * @see BufferPool
 *
 * @version 1.0
 * @author Ralph Gasser
 */
class DiskManager(val path: Path, val lockTimeout: Long = 5000) : AutoCloseable {

    companion object {
        /** Identifier of every HARE file. */
        val FILE_HEADER_IDENTIFIER = charArrayOf('H', 'A', 'R', 'E')

        /** Version of the HARE file. */
        const val FILE_HEADER_VERSION = 1.toByte()

        /** Flag for when the file's sanity can be considered OK (i.e. it was closed correctly). */
        const val FILE_SANITY_OK = 0.toByte()

        /** Flag for when the file's sanity requires a check (i.e. it was not closed correctly). */
        const val FILE_SANITY_CHECK = 1.toByte()

        /** Size of the HARE file header, which is effectively [Page] 0 in the file. */
        const val FILE_HEADER_SIZE_BYTES = 4096

        /**
         * Initializes a new HARE file. Fails, if such a file already exists.
         *
         * @param path [Path] pointing to the new HARE file.
         */
        fun init(path: Path) {
            val fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SYNC, StandardOpenOption.SPARSE, StandardOpenOption.CREATE_NEW)
            val buffer = ByteBuffer.allocate(FILE_HEADER_SIZE_BYTES)
            buffer.putChar(0, 'H')       /* Identifier. */
            buffer.putChar(2, 'A')
            buffer.putChar(4, 'R')
            buffer.putChar(6, 'E')
            buffer.put(8, FILE_HEADER_VERSION)  /* Version of the HARE format. */
            buffer.put(9, FILE_SANITY_OK)       /* Sanity byte; 0 if file was properly closed, 1 if not.  */
            buffer.putLong(10, 0L)        /* Page counter; number of pages. */
            buffer.putInt(18, 0)         /* Page counter; number of freed pages. */
            buffer.putLong(22, 0L)        /* CRC32 checksum for HARE file. */
            fileChannel.write(buffer)
            fileChannel.close()
        }

        /**
         * Tries to acquire a file lock of this [FileChannel] and returns it.
         *
         * @param timeout The amount of milliseconds to wait for lock.
         * @return lock The [FileLock] acquired.
         */
        private fun acquireFileLock(channel: FileChannel, timeout: Long) : FileLock {
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

    /** The [FileChannel] used to access this [HareColumnFile]. */
    private val fileChannel = FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SYNC, StandardOpenOption.SPARSE)

    /** Acquires an exclusive [FileLock] for file underlying this [FileChannel]. Makes sure, that no other process uses the same [HareColumnFile]. */
    private val fileLock = acquireFileLock(this.fileChannel, this.lockTimeout)

    /** Flag indicating, that */
    private val closed: AtomicBoolean = AtomicBoolean(false)

    /** A fixed 4096 byte [ByteBuffer] used to provide access to the header of the HARE file managed by this [DiskManager]. */
    private val headerBuffer = ByteBuffer.allocateDirect(FILE_HEADER_SIZE_BYTES)

    /** Total number of [Page]s maintained by this [DiskManager]. */
    var pages: Long
        @Synchronized
        get() {
            return this.headerBuffer.getLong(10)
        }

        @Synchronized
        private set(v) {
            this.headerBuffer.putLong(10, v)
            this.fileChannel.write(this.headerBuffer, 0L)
            this.headerBuffer.rewind()
        }

    /** Total number of freed [Page]s managed by this [DiskManager]. */
    var freed: Int
        @Synchronized
        get() {
            return this.headerBuffer.getInt(18)
        }

        @Synchronized
        private set(v) {
            this.headerBuffer.putInt(18, v)
            this.fileChannel.write(this.headerBuffer, 0L)
            this.headerBuffer.rewind()
        }

    /** Total number of used [Page]s. */
    val used: Long
        @Synchronized
        get() {
            return this.headerBuffer.getLong(10) - this.headerBuffer.getInt(18)
        }

    /** Returns the size of the file managed by this [DiskManager]. */
    val size
        get() = MemorySize(this.fileChannel.size().toDouble(), Units.BYTE)

    /* Make some initial sanity checks regarding the file's content. */
    init {
        this.fileChannel.read(this.headerBuffer)
        this.headerBuffer.rewind()

        /** Make necessary check on startup. */
        assert(headerBuffer.char == FILE_HEADER_IDENTIFIER[0])
        assert(headerBuffer.char == FILE_HEADER_IDENTIFIER[1])
        assert(headerBuffer.char == FILE_HEADER_IDENTIFIER[2])
        assert(headerBuffer.char == FILE_HEADER_IDENTIFIER[3])
        assert(headerBuffer.get() == FILE_HEADER_VERSION)
        assert(this.pages >= 0)

        if (headerBuffer.get() != FILE_SANITY_OK) {
           /* TODO: Perform sanity check. */
        }

        /** Update sanity flag to FILE_SANITY_CHECK at position 9. If file isn't closed properly, this flag will remain*/
        this.fileChannel.write(this.headerBuffer.put(9, FILE_SANITY_CHECK), 0)
        this.headerBuffer.rewind()
    }


    /**
     * Fetches the data identified by the given [PageId] into the given [Page] object thereby
     * replacing the content of that [Page].
     *
     * @param id [PageId] to fetch data for.
     * @param into [Page] to fetch data into. Its content will be updated.
     */
    fun read(id: PageId, page: Page) {
        this.fileChannel.read(page.data, this.pageIdToPosition(id))
        page.id = id
        page.dirty = false
        page.data.rewind()
    }

    /**
     * Updates the [Page] in the HARE file managed by this [DiskManager].
     *
     * @param page [Page] to update.
     */
    fun update(page: Page) {
        this.fileChannel.write(page.data, this.pageIdToPosition(page.id))
        page.dirty = false
        page.data.rewind()
    }

    /**
     * Appends the [Page] to the HARE file managed by this [DiskManager].
     *
     * @param page [Page] to append. Its [PageId] and flags will be updated.
     */
    fun append(page: Page) {
        val pageId = ++this.pages
        this.fileChannel.write(page.data, this.pageIdToPosition(pageId))
        page.id = pageId
        page.dirty = false
        page.data.rewind()
    }

    /**
     * Frees the [Page] identified by the given [PageId].
     *
     * @param pageId The [PageId] of the [Page] that should be freed.
     */
    fun free(pageId: PageId) {

    }

    /**
     * Closes this [DiskManager], releasing the underlying file.
     */
    override fun close() {
        if (this.closed.compareAndSet(false, true)) {
            this.headerBuffer.put(9, FILE_SANITY_OK)
            this.fileChannel.write(this.headerBuffer.rewind(), 0) /* Set sanity byte and update page counter. */
            this.fileLock.release()
            this.fileChannel.close()
        }
    }

    /**
     * Converts the given [PageId] to an offset into the file managed by this [DiskManager]. Calling this method
     * also makes necessary sanity checks regarding the file's channel status and pageId bounds.
     *
     * @param pageId The [PageId] to translate to a position.
     * @return The offset into the file.
     */
    private fun pageIdToPosition(pageId: PageId): Long {
        if (this.closed.get()) throw IllegalStateException("DiskManager for {${this.path}} was closed and cannot be used to access data.")
        if (pageId > this.pages || pageId < 1) throw PageIdOutOfBoundException(pageId, this)
        return pageId shl Page.Constants.PAGE_BIT_SHIFT
    }
}