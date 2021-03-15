package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DateValue
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.utilities.extensions.read

/**
 * A [HareSerializer] for HARE based [_root_ide_package_.org.vitrivr.cottontail.model.values.DateValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DateValueHareSerializer: HareSerializer<DateValue> {
    override val fixed: Boolean = true
    override fun serialize(page: HarePage, offset: Int, value: DateValue) {
        page.putLong(offset, value.value)
    }
    override fun deserialize(page: HarePage, offset: Int): DateValue = DateValue(page.getLong(offset))
    override fun deserialize(page: HarePage, offset: Int, size: Int): Array<DateValue> = page.lock.read {
        val buffer = page.buffer.duplicate().position(offset)
        Array(size) { DateValue(buffer.long) }
    }
}