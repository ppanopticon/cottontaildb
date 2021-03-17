package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.DateValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page

/**
 * A [HareSerializer] for HARE based [_root_ide_package_.org.vitrivr.cottontail.model.values.DateValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DateValueHareSerializer: HareSerializer<DateValue> {
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: DateValue) {
        page.putLong(offset, value.value)
    }

    override fun deserialize(page: Page, offset: Int): DateValue = DateValue(page.getLong(offset))
    override fun deserialize(page: Page, offset: Int, size: Int): Array<DateValue> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { DateValue(buffer.long) }
    }
}