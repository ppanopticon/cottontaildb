package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.ShortValue
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.utilities.extensions.read

/**
 * A [HareSerializer] for HARE based [ShortValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ShortValueHareSerializer: HareSerializer<ShortValue> {
    override val fixed: Boolean = true
    override fun serialize(page: HarePage, offset: Int, value: ShortValue) {
        page.putShort(offset, value.value)
    }
    override fun deserialize(page: HarePage, offset: Int): ShortValue = ShortValue(page.getShort(offset))
    override fun deserialize(page: HarePage, offset: Int, size: Int): Array<ShortValue> = page.lock.read {
        val buffer = page.buffer.duplicate().position(offset)
        Array(size) { ShortValue(buffer.short) }
    }
}