package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.FloatValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page

/**
 * A [HareSerializer] for HARE based [FloatValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FloatValueHareSerializer: HareSerializer<FloatValue> {
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: FloatValue) {
        page.putFloat(offset, value.value)
    }

    override fun deserialize(page: Page, offset: Int): FloatValue = FloatValue(page.getFloat(offset))
    override fun deserialize(page: Page, offset: Int, size: Int): Array<FloatValue> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { FloatValue(buffer.float) }
    }
}