package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
/**
 * A [HareSerializer] for HARE based [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleVectorValueHareSerializer(val logicalSize: Int): HareSerializer<DoubleVectorValue> {
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: DoubleVectorValue) {
        page.putDoubles(offset, value.data)
    }

    override fun deserialize(page: Page, offset: Int): DoubleVectorValue = DoubleVectorValue(page.getDoubles(offset, DoubleArray(this.logicalSize)))
    override fun deserialize(page: Page, offset: Int, size: Int): Array<DoubleVectorValue> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { DoubleVectorValue(DoubleArray(this.logicalSize) { buffer.double }) }
    }
}