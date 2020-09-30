package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer

object DoubleValueSerializer : Serializer<DoubleValue> {

    override val physicalSize: Int = Long.SIZE_BYTES
    override val logicalSize: Int = -1

    override fun deserialize(input: DataInput2, available: Int): DoubleValue = DoubleValue(input.readDouble())
    override fun serialize(out: DataOutput2, value: DoubleValue) {
        out.writeDouble(value.value)
    }

    override fun serialize(page: Page, offset: Int, value: DoubleValue) {
        page.putDouble(offset, value.value)
    }

    override fun deserialize(page: Page, offset: Int): DoubleValue = DoubleValue(page.getDouble(offset))
}