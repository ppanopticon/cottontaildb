package org.vitrivr.cottontail.database.logging.serializers

import org.vitrivr.cottontail.database.logging.operations.Operation
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 *
 */
abstract class Serializer<T: Operation> {
    /** [ByteBuffer] for pre-read. */
    private val prefixBuffer = ByteBuffer.allocate(8)

    /**
     * Returns the serialized size of the [Operation] [T].
     *
     * @param operation [Operation] [T] to gauge.
     * @return Serialized size of [Operation] [T]
     */
    abstract fun sizeOf(operation: T): Int

    /**
     * Serializes the [Operation] [T] and writes it to a new [ByteBuffer].
     *
     * @param operation [Operation] [T] to serialize
     */
    abstract fun serialize(operation: T): ByteBuffer

    /**
     * Deserializes an [Operation] [T] from the given [ByteBuffer].
     *
     * @param buffer [ByteBuffer] to deserialize from.
     */
    abstract fun deserialize(buffer: ByteBuffer): T

    /**
     * Serializes the [Operation] [T] and writes it to the given [FileChannel].
     *
     * @param operation [Operation] [T] to serialize
     * @param channel [FileChannel] to write [Operation] [T] to.
     */
    fun serialize(operation: T, channel: FileChannel) {
        val buffer = this.serialize(operation)
        this.prefixBuffer.clear().putInt(buffer.capacity())
        channel.write(this.prefixBuffer.flip())
        channel.write(buffer)
    }

    /**
     * Deserializes the [Operation] [T] and writes it to the given [FileChannel].
     *
     * @param channel [FileChannel] to read [Operation] [T] from.
     */
    fun deserialize(channel: FileChannel): T {
        channel.read(this.prefixBuffer.clear())
        val buffer = ByteBuffer.allocate(this.prefixBuffer.flip().int)
        channel.read(buffer)
        return deserialize(buffer.clear())
    }
}