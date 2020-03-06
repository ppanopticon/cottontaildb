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
import java.util.concurrent.atomic.AtomicLong


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

        /** Size of the HARE file header. Only 18 bytes are used so far. Rest is reserved for the future. */
        const val FILE_HEADER_SIZE_BYTES = 32L

        /**
         * Initializes a new HARE file. Fails, if such a file already exists.
         *
         * @param path [Path] pointing to the new HARE file.
         */
        fun init(path: Path) {
            val fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SYNC, StandardOpenOption.SPARSE, StandardOpenOption.CREATE_NEW)
            val buffer = ByteBuffer.allocate(32)
            buffer.putChar(0, 'H')    /* Identifier. */
            buffer.putChar(2, 'A')
            buffer.putChar(4, 'R')
            buffer.putChar(6, 'E')
            buffer.put(8, FILE_HEADER_VERSION)  /* Version of the HARE format. */
            buffer.put(9, FILE_SANITY_OK)       /* Sanity byte; 0 if file was properly closed, 1 if not.  */
            buffer.putLong(10, 0L)        /* Page counter; number of pages. */
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

    /**
     * The page counter. This value is volatile and only persisted when closing the [DiskManager].
     *
     * Its true value can be reconstructed by page enumeration, if the file was not properly closed.
     */
    private val pageCounter = AtomicLong(0L)

    /** Returns the number of [Page]s held by file managed by this [DiskManager]. */
    val pages
        get() = this.pageCounter.get()

    /** Returns the size of the file managed by this [DiskManager]. */
    val size
        get() = MemorySize(this.fileChannel.size().toDouble(), Units.BYTE)

    /* Make some initial sanity checks regarding the file's content. */
    init {
        val tmp = ByteBuffer.allocate(FILE_HEADER_SIZE_BYTES.toInt())
        this.fileChannel.read(tmp)
        tmp.rewind()

        /** Make necessary check on startup. */
        assert(tmp.char == FILE_HEADER_IDENTIFIER[0])
        assert(tmp.char == FILE_HEADER_IDENTIFIER[1])
        assert(tmp.char == FILE_HEADER_IDENTIFIER[2])
        assert(tmp.char == FILE_HEADER_IDENTIFIER[3])
        assert(tmp.get() == FILE_HEADER_VERSION)

        if (tmp.get() == FILE_SANITY_OK) {
            this.pageCounter.set(tmp.long)
            assert(this.pageCounter.get() >= 0L)
        } else {
            /* TODO: File wasn't properly closed. Run sanity check. */
        }

        /** Update sanity flag to FILE_SANITY_CHECK at position 9. If file isn't closed properly, this flag will remain*/
        this.fileChannel.write(ByteBuffer.allocate(1).put(FILE_SANITY_CHECK), 9)
    }


    /**
     * Fetches the data identified by the given [PageId] into the given [Page] object thereby
     * replacing the content of that [Page].
     *
     * @param id [PageId] to fetch.
     * @param into [Page] to fetch data into. Its content will be updated.
     */
    fun read(id: PageId, page: Page) {
        this.fileChannel.read(page.data, this.pageIdToPosition(id))
        page.id = id
        page.flags = (page.flags and Page.Constants.MASK_DIRTY.inv())
    }

    /**
     * Updates the [Page] in the HARE file managed by this [DiskManager].
     *
     * @param page [Page] to update. Its flags will be updated.
     */
    fun update(page: Page) {
        this.fileChannel.write(page.data, this.pageIdToPosition(page.id))
        page.flags = (page.flags and Page.Constants.MASK_DIRTY.inv())
    }

    /**
     * Appends the [Page] to the HARE file managed by this [DiskManager].
     *
     * @param page [Page] to append. Its [PageId] and flags will be updated.
     */
    fun append(page: Page) {
        val pageId = this.pageCounter.getAndIncrement()
        this.fileChannel.write(page.data, this.pageIdToPosition(pageId))
        page.id = pageId
        page.flags = (page.flags and Page.Constants.MASK_DIRTY.inv())
    }

    /**
     * Closes this [DiskManager], releasing the underlying file.
     */
    override fun close() {
        if (this.closed.compareAndSet(false, true)) {
            val buffer = ByteBuffer.allocate(9).put(FILE_SANITY_OK).putLong(this.pageCounter.get()).rewind()
            this.fileChannel.write(buffer, 9) /* Set sanity byte and update page counter. */
            this.fileLock.release()
            this.fileChannel.close()
        }
    }

    /**
     * Converts the given [PageId] to an offset into the HARE file. Calling this method also makes
     * necessary sanity checks regarding the file channel status and ID bounds.
     */
    private fun pageIdToPosition(id: PageId): Long {
        if (this.closed.get()) throw IllegalStateException("DiskManager for {${this.path}} was closed and cannot be used to access data.")
        if (id >= this.pageCounter.get() || id < 0) throw PageIdOutOfBoundException(id, this)
        return FILE_HEADER_SIZE_BYTES + id * Page.Constants.PAGE_DATA_SIZE_BYTES
    }
}