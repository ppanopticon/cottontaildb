package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page

/**
 * A [HareSerializer] for HARE based [FloatVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorValueHareSerializer(val logicalSize: Int): HareSerializer<FloatVectorValue> {
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: FloatVectorValue) {
        page.putFloats(offset, value.data)
    }

    override fun deserialize(page: Page, offset: Int): FloatVectorValue = FloatVectorValue(page.getFloats(offset, FloatArray(this.logicalSize)))
    override fun deserialize(page: Page, offset: Int, size: Int): Array<FloatVectorValue> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { FloatVectorValue(FloatArray(this.logicalSize) { buffer.float }) }
    }
}