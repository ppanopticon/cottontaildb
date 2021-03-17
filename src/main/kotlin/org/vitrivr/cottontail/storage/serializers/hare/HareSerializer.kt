package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage

/**
 * A serializer for a HARE column file.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface HareSerializer<T: Value> {

    /** */
    val fixed: Boolean

    /**
     * Serializes a value to the given [HarePage] at the given [offset].
     *
     * @param page [HarePage] to deserialize.
     * @param offset Offset in bytes from the start of the [HarePage]
     * @param value The value [T] to serialize
     */
    fun serialize(page: Page, offset: Int, value: T)

    /**
     * Deserializes a single value from the given [HarePage] at the given [index] and returns it.
     *
     * @param page [HarePage] to deserialize.
     * @param offset Offset in bytes from the start of the [HarePage]
     * @return [T] or null
     */
    fun deserialize(page: Page, offset: Int): T

    /**
     * Deserializes all values from the given [HarePage] and returns them.
     *
     * @param page [HarePage] to deserialize.
     * @param offset Offset in bytes from the start of the [HarePage].
     * @param size The number of values to deserialize.
     * @return Array of values [T].
     */
    fun deserialize(page: Page, offset: Int, size: Int): Array<T>
}