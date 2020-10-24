package org.vitrivr.cottontail.storage.engine.hare.disk

import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.PageId
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.*

/**
 * A view on a stack of [Long]s. A data structure used in HARE [DiskManager]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongStack(val buffer: ByteBuffer) {

    /** Number of freed pages, i.e., size of freed pages stack. */
    val size: Int
        get() = this.buffer.getInt(0)

    /**
     * Offers a [Long] to add to the [LongStack].
     *
     * @param pageId The [Long] to add to the stack.
     * @return True, if [Long] could be accepted, false if stack is full.
     */
    fun offer(pageId: Long): Boolean {
        val position = this.size
        val offset = Int.SIZE_BYTES + position * Long.SIZE_BYTES
        if (offset + Long.SIZE_BYTES > this.buffer.capacity()) {
            return false /* Stack of freed pages if full. */
        }
        this.buffer.putInt(0, position + 1)
        this.buffer.putLong(offset, pageId)
        return true
    }

    /**
     * Pops and returns a [Long] from the [LongStack].
     *
     * @return The [Long] of the freed page.
     */
    fun pop(): Long {
        val position = this.size - 1
        require(position >= 0) { "LongStack is empty." }
        val offset = Int.SIZE_BYTES + position * Long.SIZE_BYTES
        val pageId = this.buffer.getLong(offset)
        this.buffer.putInt(0, position)
        this.buffer.putLong(offset, 0L)
        return pageId
    }

    /**
     * Converts the current snapshot of this [LongStack] into [List] of [Long].
     *
     * @return [List] of [Long]s
     */
    fun toList(): List<Long> = (0 until this.size).map { this.buffer.getLong(Int.SIZE_BYTES + it * Long.SIZE_BYTES) }

    /**
     * Initializes this [LongStack].
     *
     * @return This [LongStack]
     */
    fun init(): LongStack {
        this.buffer.putLong(0L)
        return this
    }

    /**
     * Reads the content of this [LongStack] from disk.
     *
     * @param channel The [FileChannel] to read from.
     * @return This [LongStack]
     */
    fun read(channel: FileChannel, position: Long): LongStack {
        channel.read(this.buffer.rewind(), position)

        /** Make necessary check on reading. */
        require(this.buffer.int >= 0) { DataCorruptionException("Negative size for LongStack detected..") }
        return this
    }

    /**
     * Writes the content of this [LongStack] to disk.
     *
     * @param channel The [FileChannel] to write to.
     * @return This [LongStack]
     */
    fun write(channel: FileChannel, position: Long): LongStack {
        channel.write(this.buffer.rewind(), position)
        return this
    }
}