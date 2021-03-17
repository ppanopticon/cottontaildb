package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page

/**
 * A [HareSerializer] for HARE based [LongValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object LongValueHareSerializer: HareSerializer<LongValue> {
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: LongValue) {
        page.putLong(offset, value.value)
    }

    override fun deserialize(page: Page, offset: Int): LongValue = LongValue(page.getLong(offset))
    override fun deserialize(page: Page, offset: Int, size: Int): Array<LongValue> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { LongValue(buffer.long) }
    }
}