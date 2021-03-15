package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.utilities.extensions.read

/**
 * A [HareSerializer] for HARE based [FloatVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorValueHareSerializer(val logicalSize: Int): HareSerializer<FloatVectorValue> {
    override val fixed: Boolean = true
    override fun serialize(page: HarePage, offset: Int, value: FloatVectorValue) {
        page.putFloats(offset, value.data)
    }
    override fun deserialize(page: HarePage, offset: Int): FloatVectorValue = FloatVectorValue(page.getFloats(offset, FloatArray(this.logicalSize)))
    override fun deserialize(page: HarePage, offset: Int, size: Int): Array<FloatVectorValue> = page.lock.read {
        val buffer = page.buffer.duplicate().position(offset)
        Array(size) { FloatVectorValue(FloatArray(this.logicalSize) { buffer.float }) }
    }
}