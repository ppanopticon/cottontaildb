package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.StringValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

class FixedStringValueSerializer(override val logicalSize: Int) : Serializer<StringValue> {
    override fun deserialize(input: DataInput2, available: Int): StringValue = StringValue(input.readUTF())
    override fun serialize(out: DataOutput2, value: StringValue) {
        out.writeUTF(value.value)
    }
    override val physicalSize: Int = this.logicalSize * Int.Companion.SIZE_BYTES
    override fun serialize(page: Page, offset: Int, value: StringValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(page: Page, offset: Int): StringValue {
        TODO("Not yet implemented")
    }
}