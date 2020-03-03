package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.utilities.extensions.convertWriteLock
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.read
import it.unimi.dsi.fastutil.longs.Long2IntAVLTreeMap
import java.nio.ByteBuffer
import java.util.concurrent.locks.StampedLock

/**
 * A [BufferPool] mediates access to a HARE file through a [DiskManager] and facilitates reading and
 * writing [Page]s from/to memory and swapping [Page]s into the in-memory buffer.
 *
 * @see DiskManager
 *
 * @version 1.0
 * @author Ralph Gasser
 */
class BufferPool(val disk: DiskManager, val size: Int = 100) {

    companion object {
        const val PAGE_MEMORY_SIZE = Page.Constants.PAGE_HEADER_SIZE_BYTES + Page.Constants.PAGE_DATA_SIZE_BYTES
    }

    /** Allocates direct memory as [ByteBuffer] that is used to buffer [Page]s. This is not counted towards the heap used by the JVM. */
    private val buffer = ByteBuffer.allocateDirect(this.size * PAGE_MEMORY_SIZE)

    /** Array of [Page]s that are kept in memory. Access to this structure must be mediated. */
    private val pages = Array(this.size) {
        Page(this.buffer.position(it * PAGE_MEMORY_SIZE).limit(PAGE_MEMORY_SIZE).slice())
    }

    /** The internal directory that maps [PageId]s to [Page]s.*/
    private val pageDirectory = Long2IntAVLTreeMap()

    /** An internal lock that mediates access to the page directory. */
    private val directoryLock = StampedLock()

    /**
     * Tries to access the [Page] with the given [PageId]. If such a [Page] exists in the [BufferPool], that
     * [Page] is returned. Otherwise, the [Page] is read from the [File].
     *
     * <strong>Important:</strong> A [Page] returned by this method has its retention counter increased by 1.
     * It must be released by the caller. Otherwise, it will leak memory from the [BufferPool].
     *
     * @param pageId [Page]
     */
    fun get(pageId: PageId, priority: Page.Priority = Page.Priority.DEFAULT): Page {
        var stamp = this.directoryLock.readLock() /* Acquire non-exclusive lock. */
        try {
            var index = this.pageDirectory.getOrDefault(pageId, -1)
            if (index == -1) {
                /* Get next free page object. */
                stamp = this.directoryLock.convertWriteLock(stamp)
                index = freePage()

                /* Reset flags on page and read content from disk. */
                this.disk.read(pageId, this.pages[index])

                /* Update page directory and return page. */
                this.pageDirectory[pageId] = index
            }
            return this.pages[index].retain()
        } finally {
            this.directoryLock.unlock(stamp)
        }
    }

    /**
     * <strong>Important:</strong> A [Page] returned by this method has its retention counter set to 1.
     * It must be released by the caller. Otherwise, it will leak memory from the [BufferPool].
     */
    fun append(priority: Page.Priority = Page.Priority.DEFAULT): Page {
        val stamp = this.directoryLock.writeLock() /* Acquire exclusive lock. */
        try {
            /* Get next free page object. */
            val index = freePage()

            /* Reset flags on page and read content from disk. */
            this.disk.append(this.pages[index])

            /* Update page directory and return page. */
            this.pageDirectory[this.pages[index].id] = index
            return this.pages[index]
        } finally {
            this.directoryLock.unlock(stamp)
        }
    }


    /**
     * Flushes all dirty [Page]s to disk and resets their dirty flag. This method should be used with care,
     * since it will cause all pages to be written to disk.
     */
    fun flush() = this.directoryLock.read {
        this.pages.filter { it.dirty }.forEach {
            this.disk.update(it)
        }
    }

    /**
     * Swaps a free [Page]
     */
    private fun freePage(): Int {
        var page: Page?
        do {
            page = this.pages.filter { it.elligibleForGc }.sortedBy { it.priority.ordinal }.singleOrNull()
        } while(page == null)

        /* Remove page from page directory. */
        val index = this.pageDirectory.remove(page.id)

        /* Write page to disk if it's dirty. */
        if (page.dirty) {
            this.disk.update(page)
        }

        return index
    }
}