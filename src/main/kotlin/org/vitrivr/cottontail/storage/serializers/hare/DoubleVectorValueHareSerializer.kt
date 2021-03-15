package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.utilities.extensions.read

/**
 * A [HareSerializer] for HARE based [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleVectorValueHareSerializer(val logicalSize: Int): HareSerializer<DoubleVectorValue> {
    override val fixed: Boolean = true
    override fun serialize(page: HarePage, offset: Int, value: DoubleVectorValue) {
        page.putDoubles(offset, value.data)
    }
    override fun deserialize(page: HarePage, offset: Int): DoubleVectorValue = DoubleVectorValue(page.getDoubles(offset, DoubleArray(this.logicalSize)))
    override fun deserialize(page: HarePage, offset: Int, size: Int): Array<DoubleVectorValue> = page.lock.read {
        val buffer = page.buffer.duplicate().position(offset)
        Array(size) { DoubleVectorValue(DoubleArray(this.logicalSize) { buffer.double }) }
    }
}