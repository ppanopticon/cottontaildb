package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.ShortValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page

/**
 * A [HareSerializer] for HARE based [ShortValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ShortValueHareSerializer: HareSerializer<ShortValue> {
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: ShortValue) {
        page.putShort(offset, value.value)
    }

    override fun deserialize(page: Page, offset: Int): ShortValue = ShortValue(page.getShort(offset))
    override fun deserialize(page: Page, offset: Int, size: Int): Array<ShortValue> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { ShortValue(buffer.short) }
    }
}