package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

object DoubleValueSerializer : Serializer<DoubleValue> {

    override fun deserialize(input: DataInput2, available: Int): DoubleValue = DoubleValue(input.readDouble())
    override fun serialize(out: DataOutput2, value: DoubleValue) {
        out.writeDouble(value.value)
    }
    override val physicalSize: Int = Long.SIZE_BYTES
    override val logicalSize: Int = -1
    override fun serialize(page: Page, offset: Int, value: DoubleValue) {
        page.putDouble(offset, value.value)
    }
    override fun deserialize(page: Page, offset: Int): DoubleValue = DoubleValue(page.getDouble(offset))
}