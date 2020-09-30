package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.ShortValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer

object ShortValueSerializer : Serializer<ShortValue> {
    override fun deserialize(input: DataInput2, available: Int): ShortValue = ShortValue(input.readShort())
    override fun serialize(out: DataOutput2, value: ShortValue) {
        out.writeShort(value.value.toInt())
    }
    override val physicalSize: Int = Short.SIZE_BYTES
    override val logicalSize: Int = -1

    override fun serialize(page: Page, offset: Int, value: ShortValue) {
        page.putShort(offset, value.value)
    }

    override fun deserialize(page: Page, offset: Int): ShortValue = ShortValue(page.getShort(offset))
}