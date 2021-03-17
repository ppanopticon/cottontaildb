package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page


/**
 * A [HareSerializer] for HARE based [IntValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object IntValueHareSerializer: HareSerializer<IntValue> {
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: IntValue) {
        page.putInt(offset, value.value)
    }

    override fun deserialize(page: Page, offset: Int): IntValue = IntValue(page.getInt(offset))
    override fun deserialize(page: Page, offset: Int, size: Int): Array<IntValue> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { IntValue(buffer.int) }
    }
}