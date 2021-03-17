package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.BooleanValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page

/**
 * A [HareSerializer] for HARE based [BooleanValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object BooleanValueHareSerializer: HareSerializer<BooleanValue> {
    private const val TRUE = 1.toByte()
    private const val FALSE = 0.toByte()
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: BooleanValue) {
        page.putByte(
            offset, if (value.value) {
                TRUE
            } else {
                FALSE
            }
        )
    }

    override fun deserialize(page: Page, offset: Int): BooleanValue = BooleanValue(page.getByte(offset) == TRUE)
    override fun deserialize(page: Page, offset: Int, size: Int): Array<BooleanValue> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { BooleanValue(buffer.get() == TRUE) }
    }
}