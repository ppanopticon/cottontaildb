package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page

/**
 * A [HareSerializer] for HARE based [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DoubleValueHareSerializer: HareSerializer<DoubleValue> {
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: DoubleValue) {
        page.putDouble(offset, value.value)
    }

    override fun deserialize(page: Page, offset: Int): DoubleValue = DoubleValue(page.getDouble(offset))
    override fun deserialize(page: Page, offset: Int, size: Int): Array<DoubleValue> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { DoubleValue(buffer.double) }
    }
}