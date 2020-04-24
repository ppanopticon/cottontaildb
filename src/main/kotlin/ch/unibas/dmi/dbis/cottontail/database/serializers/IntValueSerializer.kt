package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.IntValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

object IntValueSerializer : Serializer<IntValue> {
    override val physicalSize: Int = Int.SIZE_BYTES
    override val logicalSize: Int = -1
    override fun deserialize(input: DataInput2, available: Int): IntValue = IntValue(input.readInt())
    override fun serialize(out: DataOutput2, value: IntValue) {
        out.writeInt(value.value)
    }

    override fun serialize(page: Page, offset: Int, value: IntValue) {
        page.putInt(offset, value.value)
    }

    override fun deserialize(page: Page, offset: Int): IntValue = IntValue(page.getInt(offset))
}