package org.vitrivr.cottontail.storage.engine.hare.basics

import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * A view on a [ByteBuffer] which allows for easier access as well as interaction with [FileChannels].
 *
 * @author Ralph Gasser
 * @version 1.0.1
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
     * Reads the content of this [View] from the given [FileChannel]. That [FileChannel]'s
     * position is changed by the action.
     *
     * @param channel The [FileChannel] to read from.
     */
    fun read(channel: FileChannel): View

    /**
     * Writes the content of this [View] to the given [FileChannel].
     *
     * @param channel The [FileChannel] to write to.
     */
    fun write(channel: FileChannel, position: Long): View

    /**
     * Writes the content of this [View] the given [FileChannel]. That [FileChannel]'s
     * position is changed by the action.
     *
     * @param channel The [FileChannel] to write to.
     */
    fun write(channel: FileChannel): View
}