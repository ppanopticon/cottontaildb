package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.utilities.extensions.read

/**
 * A [HareSerializer] for HARE based [LongValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object LongValueHareSerializer: HareSerializer<LongValue> {
    override val fixed: Boolean = true
    override fun serialize(page: HarePage, offset: Int, value: LongValue) {
        page.putLong(offset, value.value)
    }
    override fun deserialize(page: HarePage, offset: Int): LongValue = LongValue(page.getLong(offset))
    override fun deserialize(page: HarePage, offset: Int, size: Int): Array<LongValue> = page.lock.read {
        val buffer = page.buffer.duplicate().position(offset)
        Array(size) { LongValue(buffer.long) }
    }
}