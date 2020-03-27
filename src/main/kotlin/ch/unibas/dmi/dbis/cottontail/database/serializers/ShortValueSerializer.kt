package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.ShortValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

object ShortValueSerializer : Serializer<ShortValue> {
    override fun deserialize(input: DataInput2, available: Int): ShortValue = ShortValue(input.readShort())
    override fun serialize(out: DataOutput2, value: ShortValue) {
        out.writeShort(value.value.toInt())
    }
    override val physicalSize: Int = Short.SIZE_BYTES
    override val logicalSize: Int = -1
    override fun serialize(page: Page, offset: Int, value: ShortValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(page: Page, offset: Int): ShortValue {
        TODO("Not yet implemented")
    }
}