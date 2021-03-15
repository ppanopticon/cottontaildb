package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.utilities.extensions.read

/**
 * A [HareSerializer] for HARE based [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DoubleValueHareSerializer: HareSerializer<DoubleValue> {
    override val fixed: Boolean = true
    override fun serialize(page: HarePage, offset: Int, value: DoubleValue) {
        page.putDouble(offset, value.value)
    }
    override fun deserialize(page: HarePage, offset: Int): DoubleValue = DoubleValue(page.getDouble(offset))
    override fun deserialize(page: HarePage, offset: Int, size: Int): Array<DoubleValue> = page.lock.read {
        val buffer = page.buffer.duplicate().position(offset)
        Array(size) { DoubleValue(buffer.double) }
    }
}