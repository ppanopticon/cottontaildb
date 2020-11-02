package org.vitrivr.cottontail.storage.engine.hare.disk

import org.vitrivr.cottontail.storage.basics.MemorySize
import org.vitrivr.cottontail.storage.basics.Units
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.basics.PageConstants
import org.vitrivr.cottontail.storage.engine.hare.basics.Resource
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HareHeader
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.LongStack
import org.vitrivr.cottontail.utilities.extensions.write
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
 * The [HareDiskManager] facilitates reading and writing of [Page]s from/to the underlying HARE page file
 * usually residing on some form of persistent storage. Only one  [HareDiskManager] can be opened per HARE
 * page fil and it acquires an exclusive [FileLock] once created.
 *
 * @version 1.2.1
 * @author Ralph Gasser
 */
abstract class HareDiskManager(val path: Path, val lockTimeout: Long = 5000) : Resource {

    companion object {

        /** Minimum page shift, results in a minimum page size of 4069 bytes. */
        const val MIN_PAGE_SHIFT = 12

        /** Maximum page shift, results in a maximum page size of 4'194'304 bytes. */
        const val MAX_PAGE_SHIFT = 22

        /** [ByteBuffer] containing a 0 byte. */
        val EMPTY: ByteBuffer = ByteBuffer.allocateDirect(1)

        /** [ByteBuffer] containing [PageConstants.PAGE_TYPE_FREED]. */
        val FREED: ByteBuffer = ByteBuffer.allocateDirect(4).putInt(PageConstants.PAGE_TYPE_FREED)

        /** Offsets. */

        /** Offset into the [HareDiskManager] file to access the header.*/
        const val OFFSET_HEADER = 0L

        /** The offset into the [HareDiskManager] file to get the [LongStack] for free pages. */
        const val OFFSET_FREE_PAGE_STACK = HareHeader.SIZE.toLong()

        /**
         * Creates a new page file in the HARE format.
         *
         * @param path [Path] under which to create the page file.
         */
        fun create(path: Path, pageShift: Int = 18) {
            /* Prepare header data for page file in the HARE format. */
            val pageSize = 1 shl pageShift
            val header = HareHeader(true).init(pageShift)
            val stack = LongStack(ByteBuffer.allocateDirect(pageSize - HareHeader.SIZE)).init()

            /* Create parent directories. */
            if (Files.notExists(path.parent)) {
                Files.createDirectories(path.parent)
            }

            /* Write data to file and close. */
            val channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, StandardOpenOption.SYNC, StandardOpenOption.SPARSE)
            header.write(channel, OFFSET_HEADER)
            stack.write(channel, OFFSET_FREE_PAGE_STACK)
            channel.close()
        }
    }

    /** The [FileChannel] used to access the file managed by this [HareDiskManager]. */
    protected val fileChannel: FileChannel = FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SYNC, StandardOpenOption.SPARSE)

    /** Acquires an exclusive [FileLock] for file underlying this [FileChannel]. Makes sure, that no other process uses the same HARE file. */
    protected val fileLock = FileUtilities.acquireFileLock(this.fileChannel, this.lockTimeout)

    /** Reference to the [HareHeader] of the HARE file managed by this [HareDiskManager]. */
    protected val header = HareHeader(true).read(this.fileChannel, OFFSET_HEADER)

    /** Reference to the [LongStack] of the HARE file managed by this [HareDiskManager]. */
    protected val freePageStack: LongStack = LongStack(ByteBuffer.allocateDirect(this.pageSize - HareHeader.SIZE)).read(this.fileChannel, OFFSET_FREE_PAGE_STACK)

    /** A [ReentrantReadWriteLock] that mediates access to the closed state of this [HareDiskManager]. */
    protected val closeLock = StampedLock()

    /** The bit shift used to determine the [Page] size of the page file managed by this [HareDiskManager]. */
    val pageShift
        get() = this.header.pageShift

    /** The [Page] size of the page file managed by this [HareDiskManager]. */
    val pageSize
        get() = this.header.pageSize

    /** Number of [Page]s held by the HARE page file managed by this [HareDiskManager]. */
    val pages: Long
        get() = this.header.allocatedPages

    /** The maximum [PageId]s held by the HARE page file managed by this [HareDiskManager]. */
    val maximumPageId: PageId
        get() = this.header.maximumPageId

    /** Returns the size of the HARE page file managed by this [HareDiskManager]. */
    val size
        get() = MemorySize(this.fileChannel.size().toDouble(), Units.BYTE)

    /** Returns a list of free [PageId]s. This list is a snapshot created at time of invoking this method. */
    val freePageIds: List<PageId>
        get() = this.freePageStack.toList()

    /** Return true if this [FileChannel] and thus this [HareDiskManager] is still open. */
    override val isOpen
        get() = this.fileChannel.isOpen

    /**
     * Fetches the data identified by the given [PageId] into the given [Page] object thereby replacing the content of that [Page].
     *
     * @param pageId [PageId] to fetch data for.
     * @param page [HarePage] to fetch data into. Its content will be updated.
     */
    abstract fun read(pageId: PageId, page: HarePage)

    /**
     * Fetches the data starting from the given [PageId] into the given [Page] objects thereby replacing the content of those [Page].
     *
     * @param pageId [PageId] to start fetching
     * @param pages [Page]s to fetch data into. Their content will be updated.
     */
    abstract fun read(pageId: PageId, pages: Array<HarePage>)

    /**
     * Updates the [Page] identified by the given [PageId] in the HARE file managed by this [HareDiskManager].
     *
     * @param pageId [PageId] of the [Page] that should be updated
     * @param page [Page] the data the [Page] should be updated with.
     */
    abstract fun update(pageId: PageId, page: HarePage)

    /**
     * Allocates new [Page] in the HARE file managed by this [HareDiskManager].
     *
     * @return The [PageId] of the allocated [Page].
     */
    abstract fun allocate(): PageId

    /**
     * Frees the [Page] identified by the given [PageId] making space for new entries.
     *
     * By definition, a freed [Page] is not necessarily erased or deleted instantly (it can be, however). Nevertheless,
     * it is unsafe to use [PageId]s of freed [Page]s, until they are re-allocated, since freed [Page]s may be invalidated,
     * removed or replaced at any time.
     *
     * @param pageId The [PageId] that should be freed.
     */
    abstract fun free(pageId: PageId)

    /**
     * Commits all changes made through this [HareDiskManager].
     */
    abstract fun commit()

    /**
     * Rolls back all changes made through this [HareDiskManager].
     */
    abstract fun rollback()

    /**
     *
     */
    fun sync() {
       this.fileChannel.force(false)
    }

    /**
     * Deletes the HARE file backing this [HareDiskManager]. Calling this method also
     * closes the associated [FileChannel].
     */
    open fun delete() {
        this.close()
        Files.delete(this.path)
    }

    /**
     * Calculates the [CRC32C] checksum for the file managed by this [HareDiskManager].
     *
     * @return [CRC32C] object for this [HareDiskManager]
     */
    fun calculateChecksum(): Long {
        val page = ByteBuffer.allocateDirect(this.header.pageSize)
        val crc32 = CRC32C()
        for (i in 1L until this.pages) {
            this.fileChannel.read(page, this.pageIdToOffset(i))
            crc32.update(page.flip())
        }
        return crc32.value
    }

    /**
     * Validation method. Compares the checksum in the file's [HareHeader] to the actual checksum of the content.
     *
     * @return true If and only if checksum in header and of content are identical.
     */
    fun validate(): Boolean = (this.header.checksum == this.calculateChecksum())

    /**
     * Closes this [HareDiskManager].
     */
    final override fun close() = this.closeLock.write {
        if (this.isOpen) {
            this.prepareClose()
            this.fileChannel.force(true)
            this.fileLock.release()
            this.fileChannel.close()
        }
    }

    /**
     * Logic that should be executed prior to closing a [HareDiskManager].
     */
    protected abstract fun prepareClose()

    /**
     * Converts the given [PageId] to an offset into the file managed by this [HareDiskManager]. Calling this method
     * also makes necessary sanity checks regarding the file's channel status and pageId bounds.
     *
     * @param pageId The [PageId] to translate to a position.
     * @return The offset into the file.
     */
    protected fun pageIdToOffset(pageId: PageId): Long {
        require(pageId in 0L..this.header.maximumPageId) { "The given page ID $pageId is out of bounds for this HARE page file (file: ${this.path}, pages: ${this.pages})." }
        return pageId shl this.header.pageShift
    }
}