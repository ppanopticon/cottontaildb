package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.BooleanValue
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.utilities.extensions.read

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
    override fun serialize(page: HarePage, offset: Int, value: BooleanValue) {
        page.putByte(offset, if (value.value) { TRUE } else { FALSE })
    }
    override fun deserialize(page: HarePage, offset: Int): BooleanValue = BooleanValue(page.getByte(offset) == TRUE)
    override fun deserialize(page: HarePage, offset: Int, size: Int): Array<BooleanValue> = page.lock.read {
        val buffer = page.buffer.duplicate().position(offset)
        Array(size) { BooleanValue(buffer.get() == TRUE) }
    }
}