package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.utilities.extensions.read


/**
 * A [HareSerializer] for HARE based [IntValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object IntValueHareSerializer: HareSerializer<IntValue> {
    override val fixed: Boolean = true
    override fun serialize(page: HarePage, offset: Int, value: IntValue) {
        page.putInt(offset, value.value)
    }
    override fun deserialize(page: HarePage, offset: Int): IntValue = IntValue(page.getInt(offset))
    override fun deserialize(page: HarePage, offset: Int, size: Int): Array<IntValue> = page.lock.read {
        val buffer = page.buffer.duplicate().position(offset)
        Array(size) { IntValue(buffer.int) }
    }
}