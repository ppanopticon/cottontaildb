package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.ByteValue
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.utilities.extensions.read

/**
 * A [HareSerializer] for HARE based [ByteValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ByteValueHareSerializer: HareSerializer<ByteValue> {
    override val fixed: Boolean = true
    override fun serialize(page: HarePage, offset: Int, value: ByteValue) {
        page.putByte(offset, value.value)
    }
    override fun deserialize(page: HarePage, offset: Int): ByteValue = ByteValue(page.getByte(offset))
    override fun deserialize(page: HarePage, offset: Int, size: Int): Array<ByteValue> = page.lock.read {
        val buffer = page.buffer.duplicate().position(offset)
        Array(size) { ByteValue(buffer.get()) }
    }
}