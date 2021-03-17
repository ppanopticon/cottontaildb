package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.ByteValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page

/**
 * A [HareSerializer] for HARE based [ByteValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ByteValueHareSerializer: HareSerializer<ByteValue> {
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: ByteValue) {
        page.putByte(offset, value.value)
    }

    override fun deserialize(page: Page, offset: Int): ByteValue = ByteValue(page.getByte(offset))
    override fun deserialize(page: Page, offset: Int, size: Int): Array<ByteValue> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { ByteValue(buffer.get()) }
    }
}