package org.vitrivr.cottontail.storage.engine.hare.serializer

import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.basics.Page


/**
 * A [Serializer] that can be used to map [Value]s to byte streams and vice versa. For the
 * sake of backwards compatibility, this implementation currently extends [org.mapdb.Serializer].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Serializer<T: Value> : org.mapdb.Serializer<T> {

    /** The logical size of the [Value], i.e., the number of primitive values that make up a structured value such as an array. */
    val logicalSize: Int

    /** The physical size of the [Value] in bytes. */
    val physicalSize: Int

    /**
     * Serializes the given [Value] and writes the bytes to the [Page] at the given [offset].
     *
     * @param page [Page] to write the [Value] to.
     * @param offset The offset in bytes to write to.
     * @param value The [Value] to serialize.
     */
    fun serialize(page: Page, offset: Int, value: T)

    /**
     * Deserializes a [Value] by reading from the given [Page] at the given [offset].
     *
     * @param page [Page] to read the [Value] from.
     * @param offset The offset in bytes to read from.
     * @return Deserialized [Value]
     */
    fun deserialize(page: Page, offset: Int): T
}