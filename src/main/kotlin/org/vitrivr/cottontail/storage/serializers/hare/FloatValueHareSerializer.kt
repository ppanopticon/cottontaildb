package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.FloatValue
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.utilities.extensions.read

/**
 * A [HareSerializer] for HARE based [FloatValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FloatValueHareSerializer: HareSerializer<FloatValue> {
    override val fixed: Boolean = true
    override fun serialize(page: HarePage, offset: Int, value: FloatValue) {
        page.putFloat(offset, value.value)
    }
    override fun deserialize(page: HarePage, offset: Int): FloatValue = FloatValue(page.getFloat(offset))
    override fun deserialize(page: HarePage, offset: Int, size: Int): Array<FloatValue> = page.lock.read {
        val buffer = page.buffer.duplicate().position(offset)
        Array(size) { FloatValue(buffer.float) }
    }
}