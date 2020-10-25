package org.vitrivr.cottontail.storage.engine.hare.disk.structures

import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.basics.View
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * A [View] on a stack of [Long]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class LongStack(override val buffer: ByteBuffer) : View {

    /** Number of entries int his [LongStack]. */
    val entries: Int
        get() = this.buffer.getInt(0)

    /** The total capacity of this [LongStack]. */
    val capacity: Int = ((this.buffer.capacity() - Int.SIZE_BYTES) / Long.SIZE_BYTES)

    /**
     * Offers a [Long] to add to the [LongStack].
     *
     * @param value The [Long] to add to the stack.
     * @return True, if [Long] could be accepted, false if stack is full.
     */
    fun offer(value: Long): Boolean {
        val position = this.entries
        val offset = Int.SIZE_BYTES + position * Long.SIZE_BYTES
        if (offset + Long.SIZE_BYTES > this.buffer.capacity()) {
            return false /* Stack of freed pages if full. */
        }
        this.buffer.putInt(0, position + 1)
        this.buffer.putLong(offset, value)
        return true
    }

    /**
     * Pops and returns a [Long] from the [LongStack].
     *
     * @return The [Long] of the freed page.
     */
    fun pop(): Long {
        val position = this.entries - 1
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
    fun toList(): List<Long> = (0 until this.entries).map { this.buffer.getLong(Int.SIZE_BYTES + it * Long.SIZE_BYTES) }

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
     * Reads the content of this [LongStack] from the given [FileChannel].
     *
     * @param channel The [FileChannel] to read from.
     * @param position The position within the [FileChannel] to read at.
     * @return This [LongStack]
     */
    override fun read(channel: FileChannel, position: Long): LongStack {
        channel.read(this.buffer.rewind(), position)
        this.validate()
        return this
    }

    /**
     * Reads the content of this [LongStack] from the given [FileChannel].
     *
     * @param channel The [FileChannel] to read from.
     * @return This [LongStack]
     */
    override fun read(channel: FileChannel): View {
        channel.read(this.buffer.rewind())
        this.validate()
        return this
    }

    /**
     * Writes the content of this [LongStack] to disk.
     *
     * @param channel The [FileChannel] to write to.
     * @param position The position within the [FileChannel] to write to.
     * @return This [LongStack]
     */
    override fun write(channel: FileChannel, position: Long): LongStack {
        channel.write(this.buffer.rewind(), position)
        return this
    }

    /**
     * Writes the content of this [LongStack] to the given [FileChannel].
     *
     * @param channel The [FileChannel] to write to.
     * @return This [LongStack]
     */
    override fun write(channel: FileChannel): View {
        channel.write(this.buffer.rewind())
        return this
    }

    /**
     * Validates this [LongStack].
     */
    private fun validate() {
        /* Prepare buffer to read. */
        this.buffer.rewind()

        /* Make necessary check on reading. */
        require(this.buffer.int >= 0) { DataCorruptionException("Negative size for LongStack detected.") }
    }
}