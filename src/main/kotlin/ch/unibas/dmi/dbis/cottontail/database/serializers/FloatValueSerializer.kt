package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.FloatValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

object FloatValueSerializer : Serializer<FloatValue> {
    override val physicalSize: Int = Int.SIZE_BYTES
    override val logicalSize: Int = -1
    override fun deserialize(input: DataInput2, available: Int): FloatValue = FloatValue(input.readFloat())
    override fun serialize(out: DataOutput2, value: FloatValue) {
        out.writeFloat(value.value)
    }

    override fun serialize(page: Page, offset: Int, value: FloatValue) {
        page.putFloat(offset, value.value)
    }

    override fun deserialize(page: Page, offset: Int): FloatValue = FloatValue(page.getFloat(offset))
}