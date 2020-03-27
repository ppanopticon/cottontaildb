package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel


/**
 * A [Serializer] that can be used to map [Value]s to byte streams and vice versa. For the
 * sake of backwards compatibility, this implementation currently extends [org.mapdb.Serializer].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Serializer<T: Value> : org.mapdb.Serializer<T> {

    /** The logical size of the [Value], i.e., the number of primitive values that make up a  structured value such as an array. */
    val logicalSize: Int

    /** The physical size of the [Value] in bytes. */
    val physicalSize: Int

    /**
     * Serializes the given [Value] and writes the bytes to the [WritableByteChannel].
     *
     * @param channel [WritableByteChannel] to write the [Value] to.
     * @param value The [Value] to serialize.
     */
    fun serialize(page: Page, offset: Int, value: T)

    /**
     * Deserializes a [Value] by reading from the given [ReadableByteChannel].
     *
     * @param channel [ReadableByteChannel] to read the [Value] from.
     * @return Deserialized [Value]
     */
    fun deserialize(page: Page, offset: Int): T
}