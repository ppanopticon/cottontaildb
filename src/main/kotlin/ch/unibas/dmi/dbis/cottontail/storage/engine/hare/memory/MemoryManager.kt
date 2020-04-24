package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.memory

import ch.unibas.dmi.dbis.cottontail.storage.basics.MemorySize
import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DataPage
import java.nio.ByteBuffer

/**
 *
 */
class MemoryManager(pageSlice: Int, pages: Int) {
    /** Size of the memory allocated by this [MemoryManager]. */
    val size = MemorySize((pages shl pageSlice).toDouble(), Units.BYTE)

    /** Allocates direct memory as [ByteBuffer] that is used to buffer [DataPage]s. This is not counted towards the heap used by the JVM! */
    val buffer = ByteBuffer.allocateDirect(pages shl pageSlice)

    /** Array of [DataPage]s that are kept in memory. */
    private val pages = Array(pages) {
        this.buffer.position(it shl pageSlice).limit((it+1) shl pageSlice).slice()
    }

    /**
     * Returns the slice at the given index.
     *
     * @param index The index to return the slice for.
     */
    operator fun get(index: Int) = this.pages[index]
}