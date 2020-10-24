package org.vitrivr.cottontail.storage.engine.hare.basics

import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * A view on a [ByteBuffer] which allows for easier access as well as interaction with [FileChannels].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface View {
    /** The [ByteBuffer] referenced by this [View]. */
    val buffer: ByteBuffer

    /** Returns the size of this [View] in bytes. */
    val size
        get() = this.buffer.capacity()

    /**
     * Reads the content of this [View] from the given [FileChannel].
     *
     * @param channel The [FileChannel] to read from.
     * @param position The position in the [FileChannel] to read from to.
     */
    fun read(channel: FileChannel, position: Long): View

    /**
     * Writes the content of this [View] to disk.
     *
     * @param channel The [FileChannel] to write to.
     * @param position The position in the [FileChannel] to write to.
     */
    fun write(channel: FileChannel, position: Long): View
}